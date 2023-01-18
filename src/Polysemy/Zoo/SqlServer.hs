{-# LANGUAGE TemplateHaskell #-}

module Polysemy.Zoo.SqlServer
  (module Polysemy.Zoo.SqlServer
  ,module R
  ,Pool
  ,Connection
  )where


import Data.Pool
import Data.Vector (enumFromN)
import Database.ODBC.SQLServer
import Database.ODBC.SQLServer as R (toSql, ToSql(..), rawUnescapedText, Query, FromValue(..))
import Hoff (querySqlServer, Table)
import Hoff.SqlServer as R (AssumeUtf8Char(..), AsUtf8(..))
import Polysemy.Zoo.Prelude
import Polysemy.Zoo.Utils

data SqlServer m a where
  SqlServerQuery                :: (HasCallStack, FromRow a)
    =>                          (SqlServerSchemaName -> Query) -> SqlServer m [a]

  SqlServerTable                :: HasCallStack
    => AssumeUtf8Char ->        (SqlServerSchemaName -> Query) -> SqlServer m Table
  SqlServerExec                 :: HasCallStack
    =>                          (SqlServerSchemaName -> Query) -> SqlServer m ()

  SqlServerSchema               ::  SqlServer m SqlServerSchemaName

  -- -- this need would need Polysemy.Resource to aquire and then release it.
  -- WithSqlServerConnection       :: (Connection -> m a)                                  -> SqlServer m a
  -- -- this needs everything to use the same connection
  -- WithTransaction


data SqlServerConfig = SqlServerConfig  { scDsn                 :: Text
                                        , scPoolSize            :: Word
                                        , scPoolNumStripes      :: Maybe Int
                                        , scPoolTtlSeconds      :: Word
                                        , scSchemaName          :: SqlServerSchemaName
                                        , scQueryLevel          :: Severity
                                        , scVersionCheck        :: Maybe (Query, Text)
-- ^ first component is a query that  should yield a list of zero or one version strings,
--  which will be checked against the second component
                                        }

makeSem ''SqlServer
  
-- * Query builders
in_ :: (ToSql a, Foldable f) => Query -> f a -> Query
in_ a x | null x = rut " (1=0) "
        | True = rut " ((" <> a <>
          rut ") in (" <> mconcat (intersperse (rut ",") $ toSql <$> toList x) <> rut ")) "
  where rut = rawUnescapedText
{-# INLINABLE in_ #-}

-- | qualified table name
qtn :: IsString s => SqlServerSchemaName -> Text -> s
qtn (SqlServerSchemaName s) t = fromString $ " [" <> toS s <> "].[" <> toS t <> "] "
{-# INLINABLE qtn #-}

qts :: SqlServerSchemaName -> Text -> String
qts = qtn @String

-- * Interpreters

runSqlServerEmbedIO :: (HasCallStack, Members '[Log, Embed IO, ErrorE] r) => SqlServerConfig
  -> Sem (SqlServer ': Reader (Pool Connection) ': r) a -> Sem r a
runSqlServerEmbedIO SqlServerConfig{..} ac = do
  pool <- embed $ newPool $ setNumStripes scPoolNumStripes $ defaultPoolConfig newCon close
    (fromIntegral scPoolTtlSeconds) (fromIntegral scPoolSize)
  runReader pool $ runSqlServerAsPoolReader scSchemaName scQueryLevel ac
  where newCon = do
          conn <- connect scDsn
          forM scVersionCheck $ \(qu, v) -> do
            res <- query conn qu
            when ([v] /= res) $ ioError $ userError $ "SqlServer version mismatch. Expected '"  <>
              toS v <> "'. Got: " <> show res
          pure conn 
{-# INLINABLE runSqlServerEmbedIO #-}

runSqlServerAsPoolReader :: HasCallStack => Members '[Log, ErrorE, Embed IO, Reader (Pool Connection)] r
  => SqlServerSchemaName -> Severity -> Sem (SqlServer ': r) a -> Sem r a
runSqlServerAsPoolReader sn sev = interpret $ \case
  SqlServerSchema          -> pure sn
  SqlServerTable   utf8 qu -> withFrozenCallStack $ withConnection (querySqlServer utf8) $ qu sn
  SqlServerQuery        qu -> withFrozenCallStack $ withConnection query $ qu sn
  SqlServerExec         qu -> withFrozenCallStack $ withConnection exec $ qu sn
  where withConnection :: (HasCallStack, Members '[Log, Embed IO, ErrorE, Reader (Pool Connection)] r)
                       => (Connection -> Query -> IO a) -> Query -> Sem r a
        withConnection act q = do
          log Trace "Asking SqlServer Pool for connection ..."
          log sev $ "Executing SqlServer Query:\n" <> renderQuery q
          embedCatch . flip withResource (flip act q) =<< ask
{-# INLINABLE runSqlServerAsPoolReader #-}


-- versionCheck :: Text -> IO ()

generateSequenceValues :: Members [ErrorE, SqlServer] r => Text -> Int -> Sem r (Vector Int)
generateSequenceValues sequenceName rangeLength = 
  flip enumFromN rangeLength <$> getNextSequenceValueAndAdvance sequenceName rangeLength

-- | gets next available value in the sequence and advances the sequence by the given number of steps
getNextSequenceValueAndAdvance :: Members '[SqlServer, ErrorE] r => Text -> Int -> Sem r Int
getNextSequenceValueAndAdvance sequenceName rangeLength = do
  res <- sqlServerQuery $ \s -> [qq|
DECLARE @out sql_variant;  

EXEC sys.sp_sequence_get_range  
          @sequence_name = N'{fromSqlServerSchemaName s}.{sequenceName}'
        , @range_size = {rangeLength}
        , @range_first_value = @out OUTPUT;
  
SELECT CAST(@out as INT)|]

  case res of
    [r] -> pure r
    _ -> throw $ "Unexpected result: " <> showt res

exampleInsertId :: (Member SqlServer r, Member (Embed IO) r) => Sem r ()
exampleInsertId = do
  sqlServerExec (\_ -> [q|CREATE TABLE #a(
                    id int identity primary key,
                    a int
                    )|])
  

  replicateM_ 5 $ do
    y <- sqlServerQuery (\_ -> "INSERT INTO #a (a) OUTPUT INSERTED.ID VALUES (12);")
    print (y :: [Int])
