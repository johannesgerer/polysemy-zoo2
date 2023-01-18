{-# LANGUAGE TemplateHaskell #-}

module Polysemy.Zoo.SqlServer
  (module Polysemy.Zoo.SqlServer
  ,module R
  ,Pool
  ,Connection
  )where


import Data.Pool
import Hoff.SqlServer as R (AssumeUtf8Char(..))
import Database.ODBC.SQLServer
import Hoff (querySqlServer, Table)
import Polysemy.Zoo.Prelude
import Polysemy.Zoo.Utils

data SqlServer m a where
  SqlServerQuery        :: FromRow a =>         (SqlServerSchemaName -> Query) -> SqlServer m [a]
  SqlServerTable        :: AssumeUtf8Char ->    (SqlServerSchemaName -> Query) -> SqlServer m Table
  SqlServerExec         ::                      (SqlServerSchemaName -> Query) -> SqlServer m ()
  SqlServerSchema       ::                                                        SqlServer m SqlServerSchemaName


data SqlServerConfig = SqlServerConfig  { scDsn                 :: Text
                                        , scPoolSize            :: Word
                                        , scPoolTtlSeconds      :: Word
                                        , scSchemaName          :: SqlServerSchemaName
                                        , scQueryLevel          :: Severity
                                        , scVersionCheck        :: Maybe (Query, Text)
-- ^ first component is a query that  should yield a list of zero or one version strings,
--  which will be checked against the second component
                                        }

makeSem ''SqlServer
  
-- * Query builders
in_ :: (ToSql a, Foldable f) => f a -> Query
in_ x = rut " in (" <> mconcat (intersperse (rut ",") $ toSql <$> toList x) <> rut ") "
  where rut = rawUnescapedText
{-# INLINABLE in_ #-}

-- | qualified table name
qtn :: IsString s => SqlServerSchemaName -> Text -> s
qtn (SqlServerSchemaName s) t = fromString $ " [" <> toS s <> "].[" <> toS t <> "] "
{-# INLINABLE qtn #-}

-- * Interpreters

runSqlServerEmbedIO :: Members '[Log, Embed IO, ErrorE] r => SqlServerConfig
  -> Sem (SqlServer ': Reader (Pool Connection) ': r) a -> Sem r a
runSqlServerEmbedIO SqlServerConfig{..} ac = do
  pool <- embed $ newPool $ defaultPoolConfig newCon close
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

runSqlServerAsPoolReader :: Members '[Log, ErrorE, Embed IO, Reader (Pool Connection)] r
  => SqlServerSchemaName -> Severity -> Sem (SqlServer ': r) a -> Sem r a
runSqlServerAsPoolReader sn sev = interpret $ \case
  SqlServerSchema          -> pure sn
  SqlServerTable   utf8 qu -> withConnection (querySqlServer utf8) $ qu sn
  SqlServerQuery        qu -> withConnection query $ qu sn
  SqlServerExec         qu -> withConnection exec $ qu sn
  where withConnection :: Members '[Log, Embed IO, ErrorE, Reader (Pool Connection)] r
                       => (Connection -> Query -> IO a) -> Query -> Sem r a
        withConnection act q = do
          log Trace "Asking SqlServer Pool for connection ..."
          log sev $ "Executing SqlServer Query:\n" <> renderQuery q
          embedCatch . flip withResource (flip act q) =<< ask
{-# INLINABLE runSqlServerAsPoolReader #-}


-- versionCheck :: Text -> IO ()

