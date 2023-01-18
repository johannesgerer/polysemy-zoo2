{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# OPTIONS_GHC -fplugin=Data.Record.Anon.Plugin #-}
{-# LANGUAGE DeriveAnyClass #-}
module Polysemy.Zoo.BlockCache where

-- import           Control.Retry
import qualified Data.Aeson as A
import qualified Data.ByteString.Char8 as B8
import qualified Data.List.NonEmpty as NE
import           Data.Record.Anon.Advanced (get)
import qualified Data.Text as T
import           Data.Tuple.Select
import qualified Data.Vector as V
import           Database.SQLite.Simple as Sqlite hiding ((:=))
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

newtype PurgeExisting   = PurgeExisting Bool
  deriving Show
  deriving newtype IsBool

newtype IncludeOtherDateColumns   = IncludeOtherDateColumns Bool deriving newtype IsBool

newtype UpdateAllEntities       = UpdateAllEntities Bool deriving newtype IsBool

-- | True -> return all values between the min(date) and max(date) per entity
--
--   False -> return values for each interval, which might contain duplicates if intervals are
--   overlapping
newtype DoAggregateIntervals    = DoAggregateIntervals Bool deriving newtype IsBool

-- TODO: delete file, if initialization failed

{- * This cache can be used for data that
 1) is keyed by a key and date and has a given set of value columns
 2) can be requested in blocks of ([key], from_date) 

-}

data BlockCacheDescr = BlockCacheDescr  { bdName        :: Text
                                        , bdCols        :: NonEmpty (ColName, ColDeclaration)
                                        , bdStartDate   :: Day
                                        }
  deriving (Eq, Hashable, Generic, Show, ToJSON)

makeLensesWithSuffix ''BlockCacheDescr


-- | this function needs to populate the tables: `srNewDataTable` and `srNewDataTable <> "_errors"`
-- 
-- returns:
-- 
-- 1. number of inserted rows
-- 2. whether to purge all data for an entity before inserting the new data
type BlockSource conn m = BlockSourceRequest conn -> m (PurgeExisting, Int)




-- | f = foldable collection of blocks
--   g/h = foldable collection of rows (in a block)
--   sourceRow = column order as in SqlCacheDescr
--   errorRow = column order: bcKey, bcEntityError
type HaskellSource m f g h sourceRow errorRow = [(Day, [EntityKey])] -> m (PurgeExisting, f (g sourceRow, h errorRow))

type PythonSource r = (PythonProcess (Maybe ByteString) -> PythonProcess (Maybe ByteString)
                      -- ^ this function will be applied before running python. this can e.g. introduce
                      -- stdout/err handlers
                      , BlockSourceRequest FilePath -> Sem r
                        (PurgeExisting
                        ,(CommandLineArgs
                        -- ^ sys.args will contain this
                        ,Text)))
    -- ^ script defining a function called `requestBlocks([(day,Pandas.Series(str)])]) -> [(df,errors)]`
    -- getting passed the blocks to be requested
    -- and returns:
    --  df: None or dataframe with columns `bcDate`, `bcKey` and the value columns
    --  errors: None or dataframe  columns `bcKey` and `bcEntityError`
    --
    -- it can use datetime64ToModifiedJulian & dateColToModifiedJulian, idatetime64ToUnixEpoch

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

deriving newtype instance ToJSON Query

makeLensesWithSuffix ''BlockSourceRequest

data BlockCacheRequest = BlockCacheRequest
  { bcSeverity                  :: Severity
  -- ^ one log message will be issued on this level, and several messages will be one or two levels below
  -- this one
  , bcCacheDir                  :: FilePath
  , bcSqliteTrace               :: Maybe (Text -> IO ())
  , bcUpdateBefore               :: Maybe Timespan
    -- ^ update data on every request for a given date until UTC time passes this threshold (can be > 24h)
    --
    -- after this time. updates will only happen according to bcThresholds
    --
    -- (for a StartDateRequest: using this setting will update the data on every request.  An
    -- improvement would be a bcUpdateFrom setting, that gives the earliest time an update should be
    -- forced)
  , bcThresholds                :: NonEmpty Timespan
  -- ^ UTC thresholds (can be > 24h)
  -- 
  -- Roughly: once the current UTC time passes these thresholds the data will be refreshed.
  -- 
  -- For details see comments and code in entitiesToBeUpdated
  , bcKeepTempTables            :: KeepTempTables
  , bcIncludeOtherDateColumns   :: IncludeOtherDateColumns
  , bcUpdateAllEntities         :: UpdateAllEntities
  -- ^ determines all entities will be updated when there is at least one other update required
  , bcPayload                   :: BlockCacheRequestPayload
  }

data BlockCacheRequestPayload
  = ExactDateRequest     MissingDataHandling [(Day, EntityKey)]
  | StartDateRequest                            [(Day, EntityKey)]
  | IntervalDateRequest DoAggregateIntervals (Either (TypedTable IntervalR) [(Day, Day, EntityKey)])
  | DateListRequest     (NonEmpty Day) [EntityKey]
  
type IntervalR = '[ "from" := Day, "to" := Day, "key" := EntityKey ]

data MissingDataHandling = ForwardFillDays (Bool
   -- ^ True: treat whole row as missing if any value column is null.
                                           -- this should only get you result rows where all value are present
   --
   -- False: only forward-fill for actually missing rows. so you will get rows with missing values as
   -- long as there is at least one value present for that day
                                           , Maybe Int)
                         | LeaveMissing
                         | CutoffDate Day
                         | CutoffToday

data BlockCache m a where
  -- | constructor unsafe because it will generate wrong call site locations when logging
  UnsafeBlockCached :: (HasCallStack, SourceConn conn)
                    => BlockCacheDescr
                    -> BlockCacheRequest
                    -> BlockSource conn m
                    -> (TempTables -> Connection -> Query
                       -- ^ results query generated by `resultQuery`
                        -> IO (Int, res))
                    -> BlockCache m res
               

data TempTables' io = TempTables { tNewData         :: Query
                                 , tRequest         :: Query
                                 , tUpdateRequired  :: Query
                                 , tFinalizeNewData :: io
                                 , tCloseConn       :: io
                                 }
deriving instance Show (TempTables' ())

type TempTables = TempTables' (IO ())

makeLensesWithSuffix ''BlockCacheRequest

makeLensesWithSuffix ''BlockCacheRequestPayload

makeLensesWithSuffix ''TempTables'

instance Show TempTables where
  show tt = show $ tt { tCloseConn = (), tFinalizeNewData = () }

makeSem ''BlockCache

-- * the BlockCache actions (getting data from the store)
--

-- |
-- Nullability:
-- 1) bcKey, bcDate, bcDate2, bcDate3: I
-- 2) value columns: Maybe
-- 
tableResult :: TempTables -> Connection -> Query -> IO (Int, Table)
tableResult _ con q = do
  t <- querySqlite_ con q
  let conv c = ai $ Day . fromInt64 <$> cl c
      addDate s = bool id (s:) (V.elem s $ cols t)
      dateCols = addDate #bcDate2 $ addDate #bcDate3 [#bcDate]
  fmap (toFst count) $ runHinIO $ update (conv <$> dateCols) $ fromJusts (#bcKey : dateCols) t
   
tableResultWithErrors :: TempTables -> Connection -> Query -> IO (Int, (Table, Table))
tableResultWithErrors tt c q = (\(n,t) t2 -> (n, (t,t2))) <$> tableResult tt c q <*> errorsTable c tt

errorsTable :: Connection -> TempTables -> IO Table
errorsTable con = runHinIO . fromJusts [#bcKey, #bcEntityError] <=< querySqlite_ con . errorsQuery

errorsQuery :: TempTables -> Query
errorsQuery tt = [qc|SELECT r.bcKey, bcEntityError FROM entities e JOIN {tRequest tt} r
                     ON r.bcKey = e.bcKey
                     WHERE bcEntityError NOT NULL|]

blockCachedTable :: (HasCallStack, SourceConn conn, Members '[ErrorE, BlockCache] r)
  => BlockCacheDescr -> BlockCacheRequest -> BlockSource conn (Sem r) -> Sem r (Table, Table)
blockCachedTable desc req source =
  withFrozenCallStack unsafeBlockCached desc req source tableResultWithErrors
{-# INLINABLE blockCachedTable #-}

blockCached :: (HasCallStack, Member BlockCache r, SourceConn conn) => BlockCacheDescr
  -> BlockCacheRequest -> BlockSource conn (Sem r)
  -> (TempTables -> Connection -> Query -> IO (Int, res)) -> Sem r res
blockCached = withFrozenCallStack unsafeBlockCached
{-# INLINABLE blockCached #-}

blockCached_ :: (HasCallStack, Member BlockCache r, SourceConn conn)
  => BlockCacheDescr -> BlockCacheRequest -> BlockSource conn (Sem r) -> Sem r ()
blockCached_ desc req  = flip (withFrozenCallStack unsafeBlockCached desc req) $ pure3 $ pure (0,())
{-# INLINABLE blockCached_ #-}


type DataConsumer a row = (a, a -> row -> IO a)

blockCachedConsumeWithErrors :: (HasCallStack, SourceConn conn, Member BlockCache r, FromRow resultRow)
  => BlockCacheDescr -> BlockCacheRequest -> BlockSource conn (Sem r) -> DataConsumer res resultRow
  -> Sem r (res, Table -- ^ errors
           )
blockCachedConsumeWithErrors desc req source (init, nextRow) = unsafeBlockCached desc req source $
  \tt c q -> (\(c,t) e -> (c, (t,e))) <$> fold_ c q (0,init) (\(c,a) r -> (c+1,) <$> nextRow a r)
               <*> errorsTable c tt
{-# INLINABLE blockCachedConsumeWithErrors #-}

blockCachedConsume :: (HasCallStack, SourceConn conn, Member BlockCache r, FromRow resultRow)
  => BlockCacheDescr -> BlockCacheRequest -> BlockSource conn (Sem r) -> DataConsumer res resultRow
  -> Sem r res
blockCachedConsume desc req source (init, nextRow) = unsafeBlockCached desc req source $
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
  UnsafeBlockCached desc re getBlocksAndIngest resultAction -> do
    ins <- getInspectorT
    fmap2 snd $ measureLog (sev2 re) (\n -> "Received " <> showIns ins (fst <$> n) <> " rows from '" <> bdName desc <> "'") $ do
      (path, conn) <- measureLog (sev0 re) (\_ -> "openCacheConnection")
        $ openCacheConnection (sev2 re) False (bcCacheDir re) desc $ bcSqliteTrace re
      let sconn = toConn path conn
      tt <- measureLog (sev0 re) (\_ -> "tempSchema") $ embedCatch
        $ tempSchema (useTempTable $ R.typeOf sconn) re conn desc

      flip finallyP (embedCatch $ tCloseConn tt) $ do
        flip finallyP (embedCatch $ tFinalizeNewData tt) $ do
          updateTime <- fromBaseUtcTime <$> P.now
          requestM <- assembleBlockSourceRequest sconn conn updateTime desc re tt

          forM requestM $ \request -> do
            (purge, _) <- measureLog (sev1 re) (\(_,n) -> "Inserted " <> shot n <> " lines into newData")
              $ runTSimpleForce $ getBlocksAndIngest request

            persistNewData purge (sev1 re) updateTime tt desc conn

        chain pureT $ measureLog (sev1 re) (\_ -> "Results successfully extracted") $ embedCatch $
          resultAction tt conn =<< resultQuery tt desc re conn
{-# INLINABLE runBlockCache #-}

resultQuery :: TempTables -> BlockCacheDescr -> BlockCacheRequest -> Connection -> IO Query
resultQuery tt desc br conn = ("SELECT " <>) <$> case bcPayload br of
  DateListRequest     dates _ -> do
    execute_ conn "CREATE TEMP TABLE dates (date INTEGER)"
    withTransaction conn $
      executeMany conn "INSERT INTO dates (date) VALUES (?)" $ Only <$>
      toList (distinct $ V.fromList $ toList dates)
    pure [qc|r.bcKey, ds.date as bcDate{dd "d.bcDate"}{colList "d." desc}
            FROM {tRequest tt} r
            INNER JOIN entities e ON e.bcKey = r.bcKey
            INNER JOIN dates ds
            INNER JOIN data d ON d.bcEntityId = e.bcEntityId AND d.bcDate = ds.date
            |]
  StartDateRequest      _ -> interval "d.bcDate >= r.bcDate"
  IntervalDateRequest   _ _ -> interval "d.bcDate BETWEEN r.bcDateFrom AND r.bcDate"
  ExactDateRequest     LeaveMissing _ -> interval "d.bcDate = r.bcDate"
  -- like LeaveMissing, but will show the value at the cutoff date for any request row after the cutoff date
  ExactDateRequest     CutoffToday _ -> cutoff <$> today 
  ExactDateRequest     (CutoffDate d) _ -> pure $ cutoff d
  ExactDateRequest    (ForwardFillDays (fillNulls, days)) _ -> pure
    [qc|bcKey, requestDate as bcDate{dd "bcDate"}{colList "" desc} FROM
       (  SELECT row_number() OVER win as rn, e.bcEntityError, r.bcDate as requestDate, r.bcKey, d.*
         FROM {tRequest tt} r
         INNER JOIN entities e ON e.bcKey = r.bcKey
         INNER JOIN data d ON d.bcEntityId = e.bcEntityId
             AND d.bcDate {dateComp} r.bcDate
             {anyNulls}
         WINDOW win AS (  PARTITION BY r.bcDate, e.bcEntityId
                          ORDER BY d.bcDate DESC
                          )
       ) WHERE rn = 1
       |]
      where dateComp = maybe "<=" (\d -> "BETWEEN r.bcDate - " <> shot d <> " AND") days
            anyNulls = if fillNulls then foldr' (\(c,_) a -> a <> "AND d." <> c <> " IS NOT NULL ") ""
                                         $ bdCols desc :: Text
              else ""
  where interval cond = pure
          [qc|r.bcKey, d.bcDate{dFrom}{dd "r.bcDate"}{colList "d." desc}
             FROM {tRequest tt} r
             INNER JOIN entities e ON e.bcKey = r.bcKey
             INNER JOIN data d ON d.bcEntityId = e.bcEntityId AND {cond :: Text}
             |]
          
        dd source | isTrue (bcIncludeOtherDateColumns br)       = ", " <> (source :: Text) <> " as bcDate2"
                  | True                                        = ""
        dFrom | isTrue (bcIncludeOtherDateColumns br)   = ", bcDateFrom" :: Text
              | True                                    = ""
        cutoff (Day c) = [qc|r.bcKey, r.bcDate{dd "d.bcDate"}{colList "d." desc} FROM {tRequest tt} r
                            INNER JOIN entities e ON e.bcKey = r.bcKey
                            INNER JOIN data d ON d.bcEntityId = e.bcEntityId AND d.bcDate = min({c}, r.bcDate)
                            |]
{-# INLINABLE resultQuery #-}
    

persistNewData :: (HasCallStack, Members '[ErrorE, Log, GhcTime, Embed IO] r)
  => PurgeExisting -> Severity -> Time -> TempTables -> BlockCacheDescr -> Connection -> Sem r ()
persistNewData purge sev updateTime TempTables{..} desc conn = void $ measureLog sev
  (\(e,n) -> "Upserted " <> shot e <> " entities and " <> shot n <> " data lines")
  $ embedCatch $ withImmediateTransaction conn $ do

  -- update timestamps for existing entities and insert new entities
  mapM (flip (execute conn) $ Only updateTime) $
    ["UPDATE entities set bcLastUpdate = ? WHERE bcKey in (SELECT u.bcKey FROM " <>tUpdateRequired<>" u)"
     -- REPLACE does not work as it will generate new bcEntityId's
    ,"INSERT OR IGNORE INTO entities (bcKey, bcLastUpdate) SELECT bcKey, ? FROM " <> tUpdateRequired]
  ents <- Sqlite.changes conn
  
  -- delete all data for entities that have new data
  when (isTrue purge) $ do
    execute_ conn [qc|DELETE FROM data as d where d.bcEntityId in
                        (select e.bcEntityId from entities e where e.bcKey in
                                (select n.bcKey from {tNewData} n))|]
    -- print =<< Sqlite.changes conn

  -- persisten new data
  execute_ conn [qc|INSERT OR REPLACE INTO data (bcEntityId, bcDate{colList "" desc})
                   SELECT e.bcEntityId, n.bcDate{colList "n." desc} FROM {tNewData} n
                   LEFT JOIN entities e ON e.bcKey = n.bcKey
                   |]
  -- persist new errors
  execute_ conn [qc|UPDATE entities SET bcEntityError =
                   (SELECT n.bcEntityError FROM {tNewData}_errors n WHERE entities.bcKey = n.bcKey)
                   WHERE entities.bcKey IN (SELECT n.bcKey FROM {tNewData}_errors n)
                   |]
  (ents,) <$> Sqlite.changes conn
{-# INLINABLE persistNewData #-}
  
transformIntervals :: DoAggregateIntervals
  -> (Either (TypedTable IntervalR) [(Day, Day, EntityKey)]) -> [(Day, Day, EntityKey)]
transformIntervals = bool (either (toList . fromRecord . columnRecord) id)
                     (toList . either (aggR . fromTypedTable) (aggR . intervalsTable)) . isTrue
  where fromRecord r = V.zipWith3 (\a b c -> (a, b, c)) (get #from r) (get #to r) $ get #key r
        aggR = fmap (\(k,(f,t)) -> (f,t,k)) . unsafeH . zipDict'
               . execAggBy (zip_ (minimumA #from) (maximumA #to)) #key

intervalsTable :: ToVector f => f (Day, Day, EntityKey) -> TableH
intervalsTable (toVector -> l) = table [(#from   , toWrappedDynIorM $ sel1 <$> l)
                                       ,(#to     , toWrappedDynIorM $ sel2 <$> l)
                                       ,(#key    , toWrappedDynIorM $ sel3 <$> l)]

assembleBlockSourceRequest :: (HasCallStack, Members '[Log, GhcTime, ErrorE, Embed IO] r)
  => conn -> Connection -> Time -> BlockCacheDescr -> BlockCacheRequest -> TempTables
  -> Sem r (Maybe (BlockSourceRequest conn))
assembleBlockSourceRequest sconn conn updateTime desc bc@BlockCacheRequest{..} tt@TempTables{..} = do
  measureLog (sev1 bc) (\n -> [qq|Inserted {n} request rows|]) $ embedCatch
    $ withTransaction conn $ do
    let g q l = length l <$ executeMany conn ("INSERT INTO " <> tRequest <> " " <> q) l
        g :: ToRow a => Query -> [a] -> IO Int
    case bcPayload of
      DateListRequest         _ p -> g "(bcKey) VALUES (?)" $ coerce1' @(Only EntityKey) p
      ExactDateRequest        _ p-> g "(bcDate, bcKey) VALUES (?,?)" p
      StartDateRequest        p   -> g "(bcDate, bcKey) VALUES (?,?)" p
      IntervalDateRequest     doAgg p -> g "(bcDateFrom, bcDate, bcKey) VALUES (?,?,?)"
        $ transformIntervals doAgg p
  
  count <- measureLog (sev1 bc) (\n -> [qq|Marked all {n} entities to be updated|])
    $ embedCatch $ entitiesToBeUpdated conn tt updateTime bc
                      

  when (count > 0 && isTrue bcUpdateAllEntities)
    $ void $ measureLog (sev1 bc) (\n -> [qq|Marked all {n} entities to be updated|])
    $ embedCatch $ markAllEntitiesForUpdate conn tt

  if count == 0 then pure Nothing
    else do
    let toScR mlu = BlockSourceRequest (sev1 bc) sconn (getCutoffDate bcPayload) (coerce tNewData) desc
          $ (bdStartDate desc, [qq|SELECT bcKey FROM {tUpdateRequired} WHERE bcLastUpdate IS NULL|])
          : (minimumDateBlock <$> mlu)
    fmap (fmap (toScR . toList @Maybe . fromOnly) . headMaybe) $ embedCatch $ query_ conn
      [qq|SELECT MIN(bcLastUpdate) FROM {tUpdateRequired}|]

  where minimumDateBlock mlu =
          (add 1 $ timeToDayTruncate $ maximum (consM bcUpdateBefore bcThresholds) `substr` mlu
-- ^ in the language of entitiesToBeUpdated: this is trying to calculate the smallest date that
--                                         possibly needs updating
-- min {d| u < d + t for any t and u} = min {d| min(u) < d + max(t)} = floor(min(u) - max(t)) + 1
--
-- force:
-- min {d| n < d + f} - 1= floor(n-f) > floor(u-f) for any u (because u<n) > floor(min(u) - max(t,f))
                   ,[qq|SELECT bcKey FROM {tUpdateRequired} WHERE bcLastUpdate IS NOT NULL|])
{-# INLINABLE assembleBlockSourceRequest #-}

consM :: Foldable t => t a -> NonEmpty a -> NonEmpty a
consM = NE.prependList . toList

pythonSource :: (HasCallStack, Members '[Python] r) => PythonSource r -> BlockSource FilePath (Sem r)
pythonSource (ppTransform, getStuff) req = do
  (purge, (args, functionDef)) <- getStuff req
  fmap ((purge,) . read . B8.unpack) $ pythonByteString Nothing $ ppTransform
    (basicPp (source functionDef) $ Just $ toS $ A.encode req) { pArgs = args}
  where source functionDef = [qc|
import json
from pathlib import Path
import sqlite3
import pandas as pd
import datetime as dtl

{functionDef :: Text}

def modifiedJulianToDate(m):
  return dtl.datetime.utcfromtimestamp((m - 40587) * 86400).date()


def dateColToModifiedJulian(c):
  return datetime64ToModifiedJulian(pd.to_datetime(c))

def datetime64ToModifiedJulian(c):
  return c.astype(int) // 86400000000000 + 40587

def datetime64ToUnixEpoch(c):
  return c.astype(int) // 1000000000

def main():
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
    for (df,errors) in requestBlocks(blocks):
      # print(df)
    
      if df is not None and len(df):
        if pd.api.types.is_datetime64_any_dtype(df.bcDate):
          df.bcDate = dateColToModifiedJulian(df.bcDate)
        elif df.bcDate.dtype == dtl.date:
          df.bcDate = dateColToModifiedJulian(df.bcDate)
        else:
          assert (df.bcDate.dtype == int)
        count += df.to_sql(req['srNewDataTable'], conn, if_exists='append', index=False, chunksize=1000000)
  
      if errors is not None and len(errors):
        count += errors.to_sql(req['srNewDataTable'] + '_errors',
                               conn, if_exists='append', index=False, chunksize=1000000)
                
  # print(count, req['srConn'], req['srNewDataTable'])
  conn.close()
  Path(pythonEvalByteStringOutputFile).write_text(str(count))

main()

|]
{-# INLINABLE pythonSource #-}
  
  
-- | see documentation of `HaskellSource`
haskellSource :: (HasCallStack, Members '[Embed IO, GhcTime, Log, ErrorE] r
                 , ToRow sourceRow, ToRow errorRow, Foldable f, Foldable g, Foldable h)
  => HaskellSource (Sem r) f g h sourceRow errorRow -> BlockSource Connection (Sem r)
haskellSource getBlocks BlockSourceRequest{..} = withFrozenCallStack $ do
  requestBlocks <- forM srBlocks $ \(d, q) -> fmap ((d,) . coerce1 @(Only EntityKey))
    $ measureLog srSeverity (\n -> "Loaded " <> shot (length n) <> " new entities that need data from " <> shot d )
    $ embedCatch $ query_ srConn q
  
  (purge, resultBlocks) <- measureLog srSeverity (\bs -> "Received " <> shot (getSum $ foldMap (Sum . length) bs) <> " rows of new data")
                           $ getBlocks requestBlocks

  fmap ((purge,) . sum) $ embedCatch $ do
    forM (toList resultBlocks) $ \(dat,errs) -> withTransaction srConn $ do
      executeMany srConn [qc|INSERT INTO {srNewDataTable} (bcKey, bcDate{colList "" srCacheDescription})
                       VALUES (?,?{T.replicate (length $ bdCols srCacheDescription) ",?"})|] $ toList dat
      executeMany srConn [qc|INSERT INTO {srNewDataTable}_errors (bcKey, bcEntityError)
                       VALUES (?,?)|] $ toList errs
      Sqlite.changes srConn
{-# INLINABLE haskellSource #-}

markAllEntitiesForUpdate :: Connection -> TempTables -> IO Int
markAllEntitiesForUpdate conn tt = withTransaction conn $ execute_ conn
  [qc|INSERT INTO {tUpdateRequired tt} (bcKey, bcLastUpdate)
     SELECT bcKey, bcLastUpdate FROM entities e
     WHERE NOT EXISTS
     (SELECT 1 FROM {tUpdateRequired tt} r WHERE r.bcKey = e.bcKey) |]
  >> Sqlite.changes conn
                                 
entitiesToBeUpdated :: Connection -> TempTables -> Time -> BlockCacheRequest -> IO Int
entitiesToBeUpdated conn tt updateTime BlockCacheRequest{..} = withTransaction conn
  $ execute_ conn q >> Sqlite.changes conn
  where
    q = Query $ foldr' (\a b -> b <> "\n  OR " <> a)
      [qc|INSERT INTO {tUpdateRequired tt} (bcKey, bcLastUpdate)
         SELECT DISTINCT r.bcKey, e.bcLastUpdate FROM {tRequest tt} r
         LEFT JOIN entities e ON r.bcKey = e.bcKey WHERE
         e.bcLastUpdate IS null |] $ consM (force <$> bcUpdateBefore) $ condition <$> bcThresholds
-- how to arrive at this formula:
--
-- Assume we have data points for dates [d_1,.., d_n] ⊂ Integers and thresholds times represented by
-- T ⊂ positive Reals.
--
-- We relax that to d_1 = -infinity, d_n = D, and have update times {d + t| t ∈ T and d <= D}
-- 
-- At any given time now (n), an update has to happen, if there is at least one update time that is after
-- the last update (u) and before or equal to "now":
--
-- do_update := u < d + t <= n for any t ∈ T and d <= D
--
-- This can be transformed further:
-- do_update    = u < sup {d + t| d <= D and d+t <= n} for any t ∈ T
--              = u < sup {d| d <= D and d <= n - t} + t  for any t ∈ T
--              = u - t < min(D, floor(n - t)) for any t ∈ T
--              = u - t < min(D, floor(n - t)) for any t ∈ T
--
    condition ts = "(julianday(e.bcLastUpdate - " <> tsSeconds <> ", 'unixepoch') - 2400000.5 < "
                       <> maybe nMinusT (\up -> "min(" <> up <> ", " <> nMinusT <> ")") upper <> ")"
     where
       tsSeconds = shot $ getTimespan ts `div` nanoSecondsPerSecond
       nMinusT = shot $ getDay $ timeToDayTruncate $ substr ts updateTime

-- with force:
-- force_update := n < d + f for any d <= D
--              = n - f < D
    force th = forMaybe "True" upper $ \up ->
      let nMinusF = getTime (substr th updateTime) `div` nanoSecondsPerSecond
      in [qq|julianday({nMinusF}, 'unixepoch') - 2400000.5 < {up}|]
    upper = case bcPayload of
      StartDateRequest    _          -> Nothing
      ExactDateRequest _  _          -> Just "r.bcDate"
      DateListRequest ds _           -> Just . shot . getDay $ maximum ds
      IntervalDateRequest _ _        -> Just "r.bcDate"
{-# INLINABLE entitiesToBeUpdated #-}
  
substr :: Timespan -> Time -> Time
substr = add . invert


descriptionLines :: BlockCacheDescr -> [Text]
descriptionLines desc = [bdName desc, "Version: 1", "StartDate: " <> shot (bdStartDate desc)]
  <> toList (colDefs desc)

cacheFile :: FilePath -> BlockCacheDescr -> FilePath
cacheFile dir desc =
  dir </> toS (bdName desc <> "_" <> md5HashToHex (md5Hash Nothing $ descriptionLines desc)) <.> ".sqlite"

colDefs :: BlockCacheDescr -> NonEmpty Text
colDefs = fmap (\(x,y) -> "," <>x <> " " <> y) . bdCols

colDefsText = T.intercalate "\n        " . toList . colDefs

colList :: Text -> BlockCacheDescr -> Text
colList prefix = foldMap (((", " <> prefix) <>) . fst) . bdCols


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
  -- -- retry every second, wait a maximum of 5 seconds
  -- conn <- recoverAll (limitRetriesByCumulativeDelay 5000000 $ constantDelay 1000000) $ \_ -> open path
  conn <- open path
  forM_ doTrace (setTrace conn . Just)
  -- 10 seconds
  execute_ conn "PRAGMA busy_timeout = 10000"
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
           ,"DROP VIEW IF EXISTS entitiesView"
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
        (bcEntityId     INTEGER         NOT NULL PRIMARY KEY AUTOINCREMENT
        ,bcKey          TEXT    UNIQUE  NOT NULL
        ,bcLastUpdate   INTEGER         NOT NULL
        ,bcEntityError  TEXT            NULL
        )
        |],[q|
        CREATE VIEW dataView AS SELECT
        d.*,
        DATE(bcDate + 2400000.5,'auto') AS date,
        bcKey,
        bcEntityError,
        DATETIME(bcLastUpdate,'auto') AS lastUpdate
        FROM data d LEFT JOIN entities e ON e.bcEntityId = d.bcEntityId
         |],entitiesView]

entitiesView = [q|
        CREATE VIEW IF NOT EXISTS entitiesView AS SELECT
        e.*,
        DATETIME(bcLastUpdate,'auto') AS lastUpdate,
        count(*) as numDataRows,
        DATE(min(bcDate) + 2400000.5,'auto') AS dateMin,
        DATE(max(bcDate) + 2400000.5,'auto') AS dateMax,
        d.*
        FROM entities e LEFT JOIN data d ON d.bcEntityId = e.`bcEntityId`
        GROUP BY e.bcEntityId, bcKey, bcLastUpdate, bcEntityError
         |]

reservedColumns :: [Text]
reservedColumns = ["bcKey", "bcDateFrom", "bcDate", "bcEntityId"]

tempSchema :: Bool -> BlockCacheRequest -> Connection -> BlockCacheDescr -> IO TempTables
tempSchema useTemp BlockCacheRequest{..} conn desc = do
  suff <- Query <$> randomHex 10
  let r@TempTables{..} = TempTables ("newData" <> suff) ("request" <> suff) ("upateRequired" <> suff)
                         finNew closeConn
      dropOrRename old new' = bool ["DROP TABLE IF EXISTS " <> old]
        ["DROP TABLE IF EXISTS " <> new,"ALTER TABLE " <> old <> " RENAME TO " <> new]
                              $ isTrue bcKeepTempTables
        where new = new' <> "_debug"
      finNew = mapM_ (execute_ conn) $ dropOrRename (tNewData <> "_errors") "newErrors"
               <> dropOrRename tNewData "newData"
      closeConn = do finally (when (not reallyTemp) $ mapM_ (execute_ conn)
                              $ dropOrRename tRequest             "request"
                              <> dropOrRename tUpdateRequired      "updateRequired")
                     -- ANALYZE
                     $ void $ forkIO $ finally (execute_ conn "PRAGMA optimize") $ close conn
        
                                    
  r <$ mapM (execute_ conn) [[qc| CREATE{tt} TABLE {tNewData}_errors
                           (bcKey               TEXT NOT NULL
                           ,bcEntityError       TEXT NOT NULL
                           ) |]
                       ,[qc| CREATE{tt} TABLE {tNewData}
                           (bcKey       TEXT    NOT NULL
                           ,bcDate      INTEGER NOT NULL
                           {colDefsText desc}
                           ) |]
                       ,[qc|CREATE{tt} TABLE {tRequest}
                           (bcKey            TEXT    NOT NULL
                           ,bcDate           INTEGER
                           ,bcDateFrom       INTEGER) |]
                       ,[qc|CREATE{tt} TABLE {tUpdateRequired}
                           (bcKey            TEXT    NOT NULL
                           ,bcLastUpdate     INTEGER) |]
                       ,entitiesView]
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
