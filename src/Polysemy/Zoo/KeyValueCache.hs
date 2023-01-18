{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveAnyClass #-}
module Polysemy.Zoo.KeyValueCache where

import qualified Data.Text as T
import           Database.ODBC.SQLServer
import           Hoff as H
import           Polysemy.Zoo.Prelude
import           Polysemy.Zoo.Python
import           Polysemy.Zoo.SqlServer
import           Polysemy.Zoo.Utils

-- TODO: add krKeyColsDb, krValueColsDb (and automatically rename back and forth), but think about what
-- the krGenerateValues will expect and returns
 
-- try to get rid of the allToMaybe for keyTable (and eventually missing). the H.diff needs same types, to hits have to be cast to match keyTable
  
data KeyValueCacheDescr = KeyValueCacheDescr    { bdTable       :: TableName
                                                , bdCols        :: [ColName]
                                                }
  deriving (Eq, Hashable, Generic, Show, ToJSON)

data KeyValueCacheRequest m = KeyValueCacheRequest      { krTable               :: TableName
                                                        , krKeyCols             :: [ColName]
                                                        , krValueCols           :: [ColName]
                                                        , krKeyTable            :: Table
                                                        , krPyOdbcDsn           :: Dsn
                                                        , krGenerateValues      :: Table -> m Table
                                                        , krKeepTempTables      :: KeepTempTables
                                                        , krChunkSize           :: Word32
                                                        }


data KeyValueCache m a where
  KeyValueCached :: HasCallStack => KeyValueCacheRequest m -> KeyValueCache m Table
               
makeSem ''KeyValueCache

runKeyValueCache :: Members '[Log, ErrorE, GhcTime, Embed IO, Python, SqlServer] r
  => Sem (KeyValueCache ': r) a -> Sem r a
runKeyValueCache = interpretH $ \(KeyValueCached KeyValueCacheRequest{..}) -> do
  schema <- sqlServerSchema
  let keyColList = T.intercalate ", " $ krKeyCols
      allCols = krValueCols <> krKeyCols
      so x = mconcat ["[",fromSqlServerSchemaName schema,"].[",x,"]"]
  keyTableName' <- liftIO $ ("keyValueRequest_" <>) <$> randomHex 10
  let keyTableName = so keyTableName'
      valTableName = so krTable
      msgd t = T.unwords ["Received", shot $ count t, "hits from", krTable]
      msgi (t, g) = T.unwords ["Received", shot $ count t, "rows (" <> shot (count g), " were missing) from", krTable]

  chain (pureT . fst) $ measureLog Info msgi $ do
    -- coming from sql all hits' columns will be `Maybe`s, so cast newValues as well
    keyTable <- runH $ allToMaybe $ select (ad <$> krKeyCols) krKeyTable

    sqlServerExec $ const $ rawUnescapedText $ T.unwords
      ["SELECT TOP(0)", keyColList, "INTO", keyTableName, "FROM", so krTable]

    let dropKeyTable = when (not $ coerce krKeepTempTables) $ sqlServerExec $ const
                       $ "DROP TABLE " <> rawUnescapedText keyTableName
    hits <- flip finallyP dropKeyTable $ do

      insertMssqlServerViaPandas krChunkSize krPyOdbcDsn keyTableName' schema keyTable

      measureLog Debug msgd $
        sqlServerTable (AssumeUtf8Char True) $ const $ rawUnescapedText $ T.unwords
        ["SELECT",T.intercalate ", " $ ("v." <>) <$> allCols
        ,"FROM", valTableName, "v INNER JOIN", keyTableName
        ,"k\nON", T.intercalate " AND " $ (\x -> mconcat ["k.", x, "=v.",x]) <$> krKeyCols]

    missing <- if H.null hits then pure keyTable else do
      -- mapM (\(x,t) -> log Trace $ x <> ":\n" <> shot (meta t)) [("hits", hits), ("requested", keyTable)]
      runH $ H.diff keyTable $ xkey krKeyCols hits

    (,missing) <$> if H.null missing then pure hits else do
      log Info $ "Generating " <> shot (count missing) <> " new rows for " <> krTable
      newValues <- measureLog Debug
        (\t -> "Generated " <> shot (count t) <> " rows for " <> krTable)
        $ runTSimpleForce $ krGenerateValues missing

      insertMssqlServerViaPandas krChunkSize krPyOdbcDsn krTable schema newValues
      -- coming from sql all hits' columns will be `Maybe`s, so cast newValues as well
      runH (ujmt hits $ allToMaybe $ select (ad <$> allCols) newValues)
{-# INLINABLE runKeyValueCache #-}
  

runKeyValueCacheInIO :: PythonConfig -> SqlServerConfig
  -> Sem '[KeyValueCache, SqlServer, Reader (Pool Connection), Python, FilesystemCache
          ,Reader PythonConfig, Log, ErrorE, GhcTime, Embed IO, Final IO] a
  -> IO a
runKeyValueCacheInIO pythonConfig sqlConfig = runPythonInIONoCache pythonConfig
  . runSqlServerEmbedIO sqlConfig . runKeyValueCache
{-# INLINABLE runKeyValueCacheInIO #-}
