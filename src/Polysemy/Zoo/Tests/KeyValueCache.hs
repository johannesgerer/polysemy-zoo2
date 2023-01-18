module Polysemy.Zoo.Tests.KeyValueCache where

import           Hoff as H
import           Polysemy.Zoo.KeyValueCache
import           Polysemy.Zoo.Python
import           Polysemy.Zoo.SqlServer
import           Polysemy.Zoo.Utils

main = do
  chain (print . withMeta) $ runKeyValueCacheInIO pythonConf sqlConf $
    runKeyValueCache $ keyValueCached $ KeyValueCacheRequest
    "marketdata" ["stock","date"] ["val1","val2"]
    t1 dsnP genValues (KeepTempTables False) 1000000000

devmain = main

t1 = unsafeH $ #stock <# ["aapl","f" :: Text] // tc #date ["2023-02-23", "2022-02-23" :: Day] 
     // tc #val1 ['a','\3249'] // tc #val2 [1.23, 3.33:: Double]

genValues :: Monad m => Table -> m Table
genValues = pure . unsafeH . update [ei @Text #val1 $ ("val1_" <>) <$> #stock, #val2 </ co @Double 9.87]
  . allFromJusts

dsnP = "DRIVER=/nix/store/xkaj67gf0jnhj96ccvl8vxc2r8bqb1y1-msodbcsql17-17.7.1.1-1/lib/libmsodbcsql-17.7.so.1.1;" <> dsn
dsnH = "DRIVER=/nix/store/i8kmrjq59yhimjhvbywqqz875alsvn9g-msodbcsql17-17.7.1.1-1/lib/libmsodbcsql-17.7.so.1.1;" <> dsn
dsn = "server=localhost;Authentication=SqlPassword;UID=sa;PWD=asd@@@ASD123;Encrypt=No;database=db1;"

pythonConf = PythonConfig "python" Nothing
sqlConf = SqlServerConfig dsnH 3 300 (SqlServerSchemaName "dbo") Debug Nothing
