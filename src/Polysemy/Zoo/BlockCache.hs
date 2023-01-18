{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# OPTIONS_GHC -fplugin=Data.Record.Anon.Plugin #-}
{-# LANGUAGE DeriveAnyClass #-}
module Polysemy.Zoo.BlockCache where

import qualified Data.Aeson as A
import qualified Data.ByteString.Char8 as B8
import           Data.Record.Anon.Advanced (get)
import qualified Data.Text as T
import           Data.Tuple.Select
import qualified Data.Vector as V
import           Database.SQLite.Simple hiding ((:=))
import           Database.SQLite.Simple.FromField
import           Database.SQLite.Simple.Internal
import           Database.SQLite.Simple.Ok
import           Database.SQLite.Simple.ToField
import           Hoff hiding (invert)
import           Polysemy.Log hiding (trace)
import qualified Polysemy.State as S
import qualified Polysemy.Time as P
import           Polysemy.Zoo.Md5
import           Polysemy.Zoo.Prelude
import           Polysemy.Zoo.Python
import           Polysemy.Zoo.Utils
import           Torsor (add, invert)
import           Type.Reflection as R

newtype EntityKey = EntityKey { fromEntityKey :: Text }
  deriving (FromField, Show, Hashable, Eq, Ord, ToField, IsString) via Text

-- TODO: delete file, if initialization failed

{- * This cache can be used for data that
 1) is keyed by a key and date and has a given set of value columns
 2) can be requested in blocks of ([key], from_date) 

-}

data BlockCacheDescr = BlockCacheDescr  { bdName        :: Text
                                        , bdCols        :: [(ColName, ColDeclaration)]
                                        , bdStartDate   :: Day
                                        }
  deriving (Eq, Hashable, Generic, Show, ToJSON)

-- | this function needs to populate the given newData table
type BlockSource conn m = BlockSourceRequest conn -> m Int

type HaskellSource m f g sourceRow = [(Day, [EntityKey])] -> m (f (g sourceRow))

class Typeable a => SourceConn a where
  useTempTable :: b a -> Bool
  toConn :: FilePath -> Connection -> a

data BlockSourceRequest conn = BlockSourceRequest      { srSeverity             :: Severity
                                                       , srConn                 :: conn
                                                       , srCutoffDate           :: Maybe Day
                                                       , srNewDataTable         :: TableName
                                                       , srCacheDescription     :: BlockCacheDescr
                                                       , srBlocks               :: [(Day, Query)]
                                                       }
  deriving (ToJSON, Generic)

data BlockCacheRequest = BlockCacheRequest
  { bcSeverity                    :: Severity
-- ^ one log message will be issued on this level, and several messages will be one or two levels below this one 
  , bcCacheDir                    :: FilePath
  , bcSqliteTrace                 :: Maybe (Text -> IO ())
  , bcThresholds                  :: NonEmpty Timespan
                                     -- ^ UTC thresholds for after which a request for a day can be made or updated
  , bcKeepTempTables              :: KeepTempTables
  , bcIncludeDataDateColumn       :: IncludeDataDateColumn
  , bcUpdateAllEntities           :: UpdateAllEntities
                                     -- ^ determines all entities will be updated when there is at least one other update required
  , bcPayload                     :: BlockCacheRequestPayload
  }


data BlockCacheRequestPayload
  = ExactDateRequest     MissingDataHandling [(Day, EntityKey)]
  | StartDateRequest                            [(Day, EntityKey)]
  | IntervalDateRequest DoAggregateIntervals (Either (TypedTable IntervalR) [(Day, Day, EntityKey)])
  | DateListRequest     (NonEmpty Day) [EntityKey]
  
type IntervalR = '[ "from" := Day, "to" := Day, "key" := EntityKey ]


data MissingDataHandling = ForwardFillDays Int | LeaveMissing | CutoffDate Day

deriving newtype instance ToJSON Query

newtype IncludeDataDateColumn   = IncludeDataDateColumn Bool deriving newtype IsBool

newtype UpdateAllEntities       = UpdateAllEntities Bool deriving newtype IsBool

newtype DoAggregateIntervals    = DoAggregateIntervals Bool deriving newtype IsBool

data BlockCache m a where
  BlockCached :: (HasCallStack, SourceConn conn)
               => BlockCacheDescr
               -> BlockCacheRequest
               -> BlockSource conn m
               -> (TempTables -> Connection -> Query -> IO (Int, res))
               -> BlockCache m res
               

data TempTables = TempTables { tNewData         :: Query
                             , tRequest         :: Query
                             , tUpdateRequired  :: Query
                             , tFinalizeNewData :: IO ()
                             , tCloseConn       :: IO ()
                             }

makeSem ''BlockCache

blockCachedTable :: (HasCallStack, SourceConn conn, Members '[ErrorE, BlockCache] r)
  => BlockCacheDescr -> BlockCacheRequest -> BlockSource conn (Sem r) -> Sem r Table
blockCachedTable desc req source =  runH . update [ai $ Day . fromInt64 <$> #bcDate]
  . fromJusts [#bcDate, #bcKey] <=<
  withFrozenCallStack blockCached desc req source $ const $ fmap3 (count &&& id) querySqlite_
{-# INLINABLE blockCachedTable #-}

blockCached_ :: (HasCallStack, Member BlockCache r, SourceConn conn)
  => BlockCacheDescr -> BlockCacheRequest -> BlockSource conn (Sem r) -> Sem r ()
blockCached_ desc req  = flip (withFrozenCallStack blockCached desc req) $ pure3 $ pure (0,())
{-# INLINABLE blockCached_ #-}


type DataConsumer a row = (a, a -> row -> IO a)

blockCachedConsume :: (HasCallStack, SourceConn conn, Member BlockCache r, FromRow resultRow)
  => BlockCacheDescr -> BlockCacheRequest -> BlockSource conn (Sem r) -> DataConsumer res resultRow
  -> Sem r res
blockCachedConsume desc req source (init, nextRow) = withFrozenCallStack blockCached desc req source $
  \_ c q -> fold_ c q (0,init) (\(c,a) r -> (c+1,) <$> nextRow a r)
{-# INLINABLE blockCachedConsume #-}

runBlockCacheInIOConstant :: Maybe Severity -> Time -> s ->
  Sem '[BlockCache, Log, ErrorE , GhcTime, S.State s, Embed IO, Final IO] a -> IO a
runBlockCacheInIOConstant sev time init = runFinal . embedToFinal . S.evalState init
  . P.interpretTimeGhcConstant (toBaseUtcTime time) . errorToIOFinalThrow . interpretLogStderrLevel sev
  . runBlockCache
{-# INLINABLE runBlockCacheInIOConstant #-}

runBlockCacheInIO :: Sem '[BlockCache, Log, ErrorE , GhcTime, Embed IO, Final IO] a -> IO a
runBlockCacheInIO = runFinal . embedToFinal . interpretTimeGhc .
  errorToIOFinalThrow . interpretLogStderr . runBlockCache
{-# INLINABLE runBlockCacheInIO #-}

sev0 = lowerSev . lowerSev . bcSeverity
sev1 = lowerSev . bcSeverity
sev2 = bcSeverity

runBlockCache :: (HasCallStack, Members '[Log, ErrorE, GhcTime, Embed IO] r)
  => Sem (BlockCache ': r) a -> Sem r a 
runBlockCache = interpretH $ \case
  BlockCached desc re getBlocksAndIngest resultAction -> withFrozenCallStack $ do
    ins <- getInspectorT
    fmap2 snd $ measureLog (sev2 re) (\n -> "Received " <> showIns ins (fst <$> n) <> " rows from '" <> bdName desc <> "'") $ do
      (path, conn) <- measureLog (sev0 re) (\_ -> "openCacheConnection")
        $ openCacheConnection (sev2 re) False (bcCacheDir re) desc $ bcSqliteTrace re
      let sconn = toConn path conn
      tt <- measureLog (sev0 re) (\_ -> "tempSchema") $ embedCatch $ tempSchema (useTempTable $ R.typeOf sconn) re conn desc

      flip finallyP (embedCatch $ tCloseConn tt) $ do
        flip finallyP (embedCatch $ tFinalizeNewData tt) $ do
          updateTime <- fromBaseUtcTime <$> P.now
          requestM <- assembleBlockSourceRequest sconn conn updateTime desc re tt

          forM requestM $ \request -> do
            measureLog (sev1 re) (\n -> "Inserted " <> shot n <> " lines into newData") $ 
              runTSimpleForce $ getBlocksAndIngest request

            persistNewData (sev1 re) updateTime tt desc conn

        chain pureT $ measureLog (sev1 re) (\_ -> "Results successfully extracted") $ embedCatch $
          resultAction tt conn =<< resultQuery tt desc re conn
{-# INLINABLE runBlockCache #-}

resultQuery :: TempTables -> BlockCacheDescr -> BlockCacheRequest -> Connection -> IO Query
resultQuery tt desc BlockCacheRequest{..} conn = case bcPayload of
  DateListRequest     dates _ -> do
    execute_ conn "CREATE TEMP TABLE dates (date INTEGER)"
    withTransaction conn $
      executeMany conn "INSERT INTO dates (date) VALUES (?)" $ Only <$>
      toList (distinct $ V.fromList $ toList dates)
    pure [qc|SELECT r.bcKey, d.date as bcDate, {dd "d.bcDate"}{colList "d." desc} FROM {tRequest tt} r
            INNER JOIN entities e ON e.bcKey = r.bcKey
            INNER JOIN dates d
            INNER JOIN data d ON d.bcEntityId = e.bcEntityId AND d.bcDate = d.date
            |]
  StartDateRequest      _ -> interval "d.bcDate >= r.bcDate"
  IntervalDateRequest   _ _ -> interval "d.bcDate BETWEEN r.bcDate AND r.bcDateTo"
  ExactDateRequest     LeaveMissing _ -> interval "d.bcDate = r.bcDate"
  ExactDateRequest     (CutoffDate (Day c)) _ -> pure 
    [qc| SELECT r.bcKey, r.bcDate, {dd "d.bcDate"}{colList "d." desc} FROM {tRequest tt} r
       INNER JOIN entities e ON e.bcKey = r.bcKey
       INNER JOIN data d ON d.bcEntityId = e.bcEntityId AND d.bcDate = min({c}, r.bcDate)
       |]
  ExactDateRequest    (ForwardFillDays days) _ -> pure
    [qc|select bcKey, requestDate as bcDate, {dd "bcDate"}{colList "" desc} from
       (  SELECT row_number() OVER win as rn, r.bcDate as requestDate, r.bcKey, d.* FROM {tRequest tt} r
         INNER JOIN entities e ON e.bcKey = r.bcKey
         INNER JOIN data d ON d.bcEntityId = e.bcEntityId AND d.bcDate BETWEEN r.bcDate - {shot days} AND r.bcDate
         WINDOW win AS (  PARTITION BY r.bcDate, e.bcEntityId
                          ORDER BY d.bcDate DESC
                          )
       ) WHERE rn = 1
       |]
  where interval cond = pure [qc|SELECT r.bcKey, d.bcDate, {dd "d.bcDate"}{colList "d." desc} FROM {tRequest tt} r
                                INNER JOIN entities e ON e.bcKey = r.bcKey
                                INNER JOIN data d ON d.bcEntityId = e.bcEntityId AND {cond :: Text}
                                |]
          
        dd source | isTrue bcIncludeDataDateColumn      = (source :: Text) <> " as scDataDate, "
                  | True                                = ""
{-# INLINABLE resultQuery #-}
    

persistNewData :: (HasCallStack, Members '[ErrorE, Log, GhcTime, Embed IO] r)
  => Severity -> Time -> TempTables -> BlockCacheDescr -> Connection -> Sem r ()
persistNewData sev updateTime TempTables{..} desc conn = void $ measureLog sev
  (\(e,n) -> "Upserted " <> shot e <> " entities and " <> shot n <> " data lines")
  $ embedCatch $ withImmediateTransaction conn $ do
  mapM (flip (execute conn) $ Only updateTime) $
    ["UPDATE entities set bcLastUpdate = ? WHERE bcKey in (SELECT u.bcKey FROM " <>tUpdateRequired<>" u)"
     -- REPLACE does not work as it will generate new bcEntityId's
    ,"INSERT OR IGNORE INTO entities (bcKey, bcLastUpdate) SELECT bcKey, ? FROM " <> tUpdateRequired]
  ents <- changes conn
  execute_ conn [qc|INSERT OR REPLACE INTO data (bcEntityId, bcDate, {colList "" desc})
                   SELECT e.bcEntityId, d.bcDate, {colList "d." desc} FROM {tNewData} d
                   LEFT JOIN entities e ON e.bcKey = d.bcKey
                   |]
  (ents, ) <$> changes conn
{-# INLINABLE persistNewData #-}
  
transformIntervals :: DoAggregateIntervals
  -> (Either (TypedTable IntervalR) [(Day, Day, EntityKey)]) -> [(Day, Day, EntityKey)]
transformIntervals = bool (either (toList . fromRecord . columnRecord) id)
                     (toList . either (aggR . fromColumns) (aggF . V.fromList)) . isTrue
  where fromRecord r = V.zipWith3 (\a b c -> (a, b, c)) (get #from r) (get #to r) $ get #key r
        aggR = fmap (\(k,(f,t)) -> (f,t,k)) . unsafeH . zipDict'
               . execAggBy (zip_ (minimumA #from) (maximumA #to)) #key
        aggF l = aggR $ table [(#from   , toWrappedDynIorM $ sel1 <$> l)
                              ,(#to     , toWrappedDynIorM $ sel2 <$> l)
                              ,(#key    , toWrappedDynIorM $ sel3 <$> l)]

assembleBlockSourceRequest :: (HasCallStack, Members '[Log, GhcTime, ErrorE, Embed IO] r)
  => conn -> Connection -> Time -> BlockCacheDescr -> BlockCacheRequest -> TempTables
  -> Sem r (Maybe (BlockSourceRequest conn))
assembleBlockSourceRequest sconn conn updateTime desc bc@BlockCacheRequest{..} tt@TempTables{..} = do
  measureLog (sev1 bc) (\n -> "Inserted " <> shot n <> " request rows") $ embedCatch
    $ withTransaction conn $ do
    let g q l = length l <$ executeMany conn ("INSERT INTO " <> tRequest <> " " <> q) l
        g :: ToRow a => Query -> [a] -> IO Int
    case bcPayload of
      DateListRequest         _ p -> g "(bcKey) VALUES (?)" $ coerce1' @(Only EntityKey) p
      ExactDateRequest        _ p-> g "(bcDate, bcKey) VALUES (?,?)" p
      StartDateRequest        p   -> g "(bcDate, bcKey) VALUES (?,?)" p
      IntervalDateRequest     doAgg p -> g "(bcDate, bcDateTo, bcKey) VALUES (?,?,?)"
        $ transformIntervals doAgg p
  
  count <- measureLog (sev1 bc) (\n -> "Marked " <> shot n <> " entities to be updated")
    $ embedCatch $ entitiesToBeUpdated conn tt updateTime (toList timespans) bcPayload
                      

  when (count > 0 && isTrue bcUpdateAllEntities)
    $ void $ measureLog (sev1 bc) (\n -> "Marked all " <> shot n <> " entities to be updated")
    $ embedCatch $ markAllEntitiesForUpdate conn tt

  if count == 0 then pure Nothing else
    let toScR (Only minLastUpdate) = BlockSourceRequest (sev1 bc) sconn (getCutoffDate bcPayload) (coerce tNewData) desc $
          [(bdStartDate desc, "SELECT bcKey FROM " <> tUpdateRequired <> " WHERE bcLastUpdate IS NULL")]
          <> ffor (toList @Maybe minLastUpdate)
          (\mlu -> (timeToDayTruncate $ minimum timespans `add` mlu
                   ,"SELECT bcKey FROM " <> tUpdateRequired <> " WHERE bcLastUpdate IS NOT NULL"))
          -- ^ this gives one extra day, one day after this is what really needs updating
    in fmap (fmap toScR . headMaybe) $ embedCatch $ query_ conn $ "SELECT MIN(bcLastUpdate) FROM "
       <> tUpdateRequired

  where timespans = invert <$> bcThresholds
{-# INLINABLE assembleBlockSourceRequest #-}

pythonSource :: (HasCallStack, Members '[Python] r) =>
  (BlockSourceRequest FilePath ->
   Sem r (CommandLineArgs
           -- ^ sys.args will contain this
         ,Text))
    -- ^ script defining a function called `requestBlocks([(day,Pandas.Series(str)])]) -> [df]`
    -- getting passed the blocks to be requested
    -- and returns the data columns `bcDate`, `scEntity` and the value columns
    --
    -- it can use datetime64ToModifiedJulian & dateColToModifiedJulian, idatetime64ToUnixEpoch
  -> BlockSource FilePath (Sem r)
pythonSource getStuff req = do
  (args, functionDef) <- getStuff req
  fmap (read . B8.unpack) $ pythonByteString dontCache args (Just $ toS $ A.encode req) $
    [qc|
import json
from pathlib import Path
import sqlite3
import pandas as pd
import datetime as dtl

{functionDef}

def modifiedJulianToDate(m):
  return dtl.datetime.utcfromtimestamp((m - 40587) * 86400).date()


def dateColToModifiedJulian(c):
  return datetime64ToModifiedJulian(pd.to_datetime(c))

def datetime64ToModifiedJulian(c):
  return c.astype(int) // 86400000000000 + 40587

def datetime64ToUnixEpoch(c):
  return c.astype(int) // 1000000000

req = json.load(sys.stdin)
conn = sqlite3.connect(req['srConn'])
blocks = []
for date, query in req['srBlocks']:
  ents = pd.read_sql(query, conn)
  if len(ents):
    blocks.append((modifiedJulianToDate(date), ents.bcKey))

# print(blocks)
count = 0
if len(blocks):
  for df in requestBlocks(blocks):
    # print(df)
  
    if df is not None and len(df):
      if pd.api.types.is_datetime64_any_dtype(df.bcDate):
        df.bcDate = dateColToModifiedJulian(df.bcDate)
      elif df.bcDate.dtype == dtl.date:
        df.bcDate = dateColToModifiedJulian(df.bcDate)
      else:
        assert (df.bcDate.dtype == int)
      count += df.to_sql(req['srNewDataTable'], conn, if_exists='append', index=False, chunksize=1000000)
              
# print(count, req['srConn'], req['srNewDataTable'])
conn.close()
Path(pythonEvalByteStringOutputFile).write_text(str(count))
|]
{-# INLINABLE pythonSource #-}
  
  
-- | the order of columns in `sourceRow` is taken from the SqlCacheDescr
haskellSource :: (HasCallStack, Members '[Embed IO, GhcTime, Log, ErrorE] r
                 , ToRow sourceRow, Foldable f, Foldable g)
  => HaskellSource (Sem r) f g sourceRow -> BlockSource Connection (Sem r)
haskellSource getBlocks BlockSourceRequest{..} = withFrozenCallStack $ do
  requestBlocks <- forM srBlocks $ \(d, q) -> fmap ((d,) . coerce1 @(Only EntityKey))
    $ measureLog srSeverity (\n -> "Loaded " <> shot (length n) <> " new entities that need data from " <> shot d )
    $ embedCatch $ query_ srConn q
  
  resultBlocks <- measureLog srSeverity (\bs -> "Received " <> shot (getSum $ foldMap (Sum . length) bs) <> " rows of new data")
                  $ getBlocks requestBlocks

  fmap sum $ embedCatch $ do
    forM (toList resultBlocks) $ \dat -> withTransaction srConn $ do
      executeMany srConn [qc|INSERT INTO {srNewDataTable} (bcKey, bcDate, {colList "" srCacheDescription})
                       VALUES (?,?{T.replicate (length $ bdCols srCacheDescription) ",?"})|] $ toList dat
      changes srConn
{-# INLINABLE haskellSource #-}

markAllEntitiesForUpdate :: Connection -> TempTables -> IO Int
markAllEntitiesForUpdate conn tt = withTransaction conn $ execute_ conn
  [qc|INSERT INTO {tUpdateRequired tt} (bcKey, bcLastUpdate)
     SELECT bcKey, bcLastUpdate FROM entities e
     WHERE NOT EXISTS
     (SELECT 1 FROM {tUpdateRequired tt} r WHERE r.bcKey = e.bcKey) |]
  >> changes conn
                                 
entitiesToBeUpdated :: Connection -> TempTables -> Time -> [Timespan] -> BlockCacheRequestPayload -> IO Int
entitiesToBeUpdated conn tt updateTime timespans request = withTransaction conn
  $ execute_ conn q >> changes conn
  where
    q = Query $ 
      [qc|INSERT INTO {tUpdateRequired tt} (bcKey, bcLastUpdate)
         SELECT DISTINCT r.bcKey, e.bcLastUpdate FROM {tRequest tt} r LEFT JOIN entities e ON r.bcKey = e.bcKey WHERE
         e.bcLastUpdate IS null OR |]
        <> T.intercalate "\n  OR " (condition <$> timespans)
    condition ts = "(julianday(e.bcLastUpdate + (" <> tsSeconds <> "), 'unixepoch', 'start of day') - 2400000.5 < "
                       <> upper <> ")"
     where
       tsSeconds = shot $ getTimespan ts `div` nanoSecondsPerSecond
       allowedDate = shot $ getDay $ timeToDayTruncate $ add ts updateTime
       minWith col = "min(" <> col <> ", " <> allowedDate <> ")"
       upper = case request of
         StartDateRequest    _          -> allowedDate
         ExactDateRequest _  _          -> minWith "r.bcDate"
         DateListRequest ds _           -> minWith . shot . getDay $ maximum ds
         IntervalDateRequest _ _        -> minWith "r.bcDateTo"
{-# INLINABLE entitiesToBeUpdated #-}
  


descriptionLines :: BlockCacheDescr -> [Text]
descriptionLines desc = [bdName desc, "StartDate: " <> shot (bdStartDate desc)] <> colDefs desc

cacheFile :: FilePath -> BlockCacheDescr -> FilePath
cacheFile dir desc =
  dir </> toS (bdName desc <> "_" <> md5HashToHex (md5Hash Nothing $ descriptionLines desc)) <.> ".sqlite"

colDefs :: BlockCacheDescr -> [Text]
colDefs = fmap (\(x,y) -> "," <>x <> " " <> y) . bdCols

colDefsText = T.intercalate "\n        " . colDefs

colList :: Text -> BlockCacheDescr -> Text
colList prefix = T.intercalate ", " . fmap ((prefix <>) . fst) . bdCols


deleteBlockCache :: FilePath -> BlockCacheDescr -> IO ()
deleteBlockCache dir desc = flip when (removeFile path) =<< doesFileExist path
  where path = cacheFile dir desc

openCacheConnection :: (HasCallStack, Members '[Log, Embed IO, ErrorE] r)
  => Severity -> Bool -> FilePath -> BlockCacheDescr -> Maybe (Text -> IO ()) -> Sem r (FilePath, Connection)
openCacheConnection sev clearCache dir desc doTrace = withFrozenCallStack $ do
  ex <- embed $ doesFileExist path
  fmap (path,) $ if not ex || clearCache then do
    forM (bdCols desc) $ \(c,_) -> when (elem c reservedColumns) $ throw $ "'" <> c <> "' is a reserved column name"
    log sev ("SqlCache: creating " <> toS path)
    embedCatch $ do
      createDirectoryIfMissing True $ takeDirectory path
      (\c -> c <$ create c) =<< open'
    else do
    log (lowerSev sev) $ "Opening " <> toS path
    embedCatch $ do conn <- open'
                    -- is there are no tables yet, treat it as a new file (this happens for example if
                    -- the file has been previously opened with sqlite cli or similar)
                    tableCount <- sum @[] @Int64 . fmap fromOnly <$> query_ conn
                      "SELECT count(*) FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%'"
                    conn <$ when (tableCount == 0) (create conn)
  where path = cacheFile dir desc
        open' = openOptimized doTrace path
        create conn = do
          mapM_ (execute_ conn) $ initialSchema clearCache desc
          executeMany conn "INSERT INTO meta (value) VALUES (?)" (Only <$> descriptionLines desc)
        
{-# INLINABLE openCacheConnection #-}

openOptimized :: Maybe (Text -> IO ()) -> String -> IO Connection
openOptimized doTrace path = do
  conn <- open path
  forM_ doTrace (setTrace conn . Just)
  execute_ conn "PRAGMA temp_store=MEMORY"
  execute_ conn "PRAGMA cache_size=-1000000" -- 1 GB

  -- liftIO $ mapM_ (execute_ conn)
  --   [
  --     "PRAGMA journal_mode = OFF;"
  --   ,"PRAGMA synchronous = 0;"
  --   ,"PRAGMA locking_mode = EXCLUSIVE;"
  --   ]

  pure conn 

    
-- * Instances

deriving newtype instance ToField Day 
deriving newtype instance FromField Day 

instance ToField Timespan where
  toField = SQLInteger . (flip div nanoSecondsPerSecond) . getTimespan

instance ToField Time where
  toField = SQLInteger . (flip div nanoSecondsPerSecond) . getTime

instance FromField Time where
  fromField = fmap (Time . (* nanoSecondsPerSecond)) . takeInt

instance FromField Timespan where
  fromField = fmap (Timespan . (* nanoSecondsPerSecond)) . takeInt

nanoSecondsPerSecond = 1000*1000*1000

takeInt :: (Num a, Typeable a) => Field -> Ok a
takeInt (Field (SQLInteger i) _) = Ok . fromIntegral $ i
takeInt f                        = returnError ConversionFailed f "need an int"
{-# INLINABLE takeInt #-}

-- * Schema definition

initialSchema :: Bool -> BlockCacheDescr -> [Query]
initialSchema clearCache desc =
  (bool [] ["DROP VIEW IF EXISTS dataView"
           ,"DROP TABLE IF EXISTS entities"
           ,"DROP TABLE IF EXISTS data"
           ,"DROP TABLE IF EXISTS meta"
           ] clearCache) <>
  [[qc|
        CREATE TABLE data
        (bcEntityId     INTEGER NOT NULL
        ,bcDate         INTEGER NOT NULL
        {colDefsText desc}
        ,PRIMARY KEY (bcEntityId,bcDate) ON CONFLICT REPLACE
        ) WITHOUT ROWID

        |],[qc|
        CREATE TABLE meta
        (value TEXT)
        |],[qc|

        CREATE TABLE entities
        (bcEntityId     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
        ,bcKey          TEXT       UNIQUE NOT NULL
        ,bcLastUpdate   INTEGER    NOT NULL
        )
        |],[q|
             CREATE VIEW dataView AS SELECT
             d.*,
             DATE(bcDate + 2400000.5,'auto') AS date,
             bcKey,
             DATETIME(bcLastUpdate,'auto') AS lastUpdate
             FROM data d LEFT JOIN entities e ON e.bcEntityId = d.bcEntityId
              |]]

reservedColumns :: [Text]
reservedColumns = ["bcKey", "bcDate", "bcDateTo", "bcEntityId"]

tempSchema :: Bool -> BlockCacheRequest -> Connection -> BlockCacheDescr -> IO TempTables
tempSchema useTemp BlockCacheRequest{..} conn desc = do
  suff <- Query <$> randomHex 10
  let r@TempTables{..} = TempTables ("newData" <> suff) ("request" <> suff) ("upateRequired" <> suff)
                         finNew closeConn
      dropOrRename old new' = bool ["DROP TABLE IF EXISTS " <> old]
        ["DROP TABLE IF EXISTS " <> new,"ALTER TABLE " <> old <> " RENAME TO " <> new]
                              $ isTrue bcKeepTempTables
        where new = new' <> "_debug"
      finNew = mapM_ (execute_ conn) $ dropOrRename tNewData "newData"
      closeConn = do finally (when (not reallyTemp) $ mapM_ (execute_ conn)
                              $ dropOrRename tRequest             "request"
                              <> dropOrRename tUpdateRequired      "updateRequired")
                     $ void $ forkIO $ finally (execute_ conn "PRAGMA optimize") $ close conn 
        
                                    
  r <$ mapM (execute_ conn) [[qc| CREATE{tt} TABLE {tNewData}
                           (bcKey TEXT NOT NULL
                           ,bcDate INTEGER NOT NULL
                           {colDefsText desc}
                           ) |]
                       ,[qc|CREATE{tt} TABLE {tRequest}
                           (bcKey            TEXT    NOT NULL
                           ,bcDate           INTEGER
                           ,bcDateTo         INTEGER) |]
                       ,[qc|CREATE{tt} TABLE {tUpdateRequired}
                           (bcKey            TEXT    NOT NULL
                           ,bcLastUpdate     INTEGER) |]]
    where tt = bool @Text "" " TEMP" reallyTemp
          reallyTemp = useTemp && not (isTrue bcKeepTempTables)
{-# INLINABLE tempSchema #-}
                       
                             

                        
          
-- "DROP TABLE IF EXISTS newData"
--                        ,"DROP TABLE IF EXISTS request"
--                        ,"DROP TABLE IF EXISTS updateRequired"
--                        ,



instance SourceConn FilePath where
  useTempTable _ = False
  toConn a _ = a

instance SourceConn Connection where
  useTempTable _ = True
  toConn _ a = a

getCutoffDate :: BlockCacheRequestPayload -> Maybe Day
getCutoffDate = \case
      ExactDateRequest         (CutoffDate d) _ -> Just d
      DateListRequest          d _ -> Just $ maximum d
      _ -> Nothing
{-# INLINABLE getCutoffDate #-}
