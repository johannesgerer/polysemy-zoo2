{-# LANGUAGE TemplateHaskell #-}

module Polysemy.Zoo.Python
  (module Polysemy.Zoo.Python
  ,module Reexport) where

import           Hoff
import           Hoff as Reexport (CommandLineArgs)
import           Polysemy.Log
import           Polysemy.Zoo.FilesystemCache as Reexport
import           Polysemy.Zoo.Prelude
import           Polysemy.Zoo.Utils
import qualified Prelude as P

-- import qualified Data.ByteString as B
-- import qualified Prelude
-- import qualified System.IO as S
-- import           System.IO.Temp
-- import           System.Posix.IO.ByteString hiding (fdWrite)
-- import           System.Posix.Types
-- import           System.Process hiding (createPipe)

data Python m a where
  PythonByteString      :: HasCallStack =>
    FsCacheKey -> CommandLineArgs -> Maybe ByteString -> Text -> Python m ByteString

  PythonTable           :: (ToJSON j, HasCallStack) =>
    FsCacheKey -> CommandLineArgs -> j -> Either Table [(Text,Table)] -> Text -> Python m Table

  PythonTables          :: (ToJSON j, HasCallStack) =>
    FsCacheKey -> CommandLineArgs -> j -> Either Table [(Text,Table)] -> Text -> Python m [(Text, Table)]

data PythonConfig =  PythonConfig       { cPythonBin    :: FilePath
                                        , cPythonPath   :: Maybe String
                                        }

makeSem ''Python


runPythonInEmbedIO :: HasPythonEffects r => Sem (Python ': r) a -> Sem r a
runPythonInEmbedIO = interpret $ \case
  PythonByteString  key args input source
    -> embedPython key $ \p e -> pythonEvalByteString p source args e input
  PythonTable       key args jarg input source -> fromEither . cborToTable <=<
    embedPython key $ \p e -> pythonEvalHoffCbor p (singleTableSourceMod source <> renameOutputDfColumns)
                              args jarg e input
  PythonTables      key args jarg input source   -> fromEither . cborToTables <=<
    embedPython key $ \p e -> pythonEvalHoffCbor p (source <> renameOutputDfColumns) args jarg e input
{-# INLINABLE runPythonInEmbedIO #-}

type HasPythonEffects r = Members '[Embed IO, ErrorE, FilesystemCache, Reader PythonConfig] r

embedPython :: (HasCallStack, HasPythonEffects r) => FsCacheKey
  -> (FilePath -> Maybe [(String, String)] -> IO ByteString) -> Sem r ByteString
embedPython ck eval = let ppEnv = "PYTHONPATH" in
  ask >>= \PythonConfig{..} -> fsCachedByteString ck $ embedCatch $ do
  env <- getEnvironment
  eval cPythonBin $ ffor cPythonPath $ \ppath -> P.filter (\(n,_) -> n /= ppEnv) env ++ [(ppEnv, ppath)]
{-# INLINABLE embedPython #-}

-- | rename to camel case
-- TODO this will leave the columns unchanged if renaming introduces duplicate columns
renameOutputDfColumns :: Text
renameOutputDfColumns = [q|
import stringcase

for _,outputDf in outputDfs:

  renamed = [stringcase.camelcase(x.lower()) for x in outputDf]
  
  if 0:
    print("renaming columns from:\n", list(outputDf))
    print("to:\n", renamed)
  
  if len(set(renamed)) == len(set(outputDf.columns)):
    outputDf.columns = renamed
|]


runPythonInEmbedIONoCache :: Members '[Embed IO, ErrorE, Log, GhcTime] r => PythonConfig
  -> Sem (Python ': FilesystemCache ': Reader PythonConfig ': r) a -> Sem r a
runPythonInEmbedIONoCache config = runReader config . runFilesystemCache "/unused_cache_folder" mempty
  mempty . runPythonInEmbedIO
{-# INLINABLE runPythonInEmbedIONoCache #-}

runPythonInIONoCache :: PythonConfig ->
  Sem '[Python, FilesystemCache, Reader PythonConfig, Log, ErrorE , GhcTime, Embed IO, Final IO] a -> IO a
runPythonInIONoCache config = runFinal . embedToFinal . interpretTimeGhc . errorToIOFinalThrow
  . interpretLogStderr . runPythonInEmbedIONoCache config
{-# INLINABLE runPythonInIONoCache #-}



insertMssqlServerViaPandas :: (HasCallStack, Members '[Embed IO, GhcTime, Log, ErrorE, Python] r)
  => Word32 -> Text -> TableName -> SqlServerSchemaName -> Table -> Sem r ()
{-# INLINABLE insertMssqlServerViaPandas #-}
insertMssqlServerViaPandas chunkSize dsn tableName schemaName table = void $ measureLog Debug
  (\_ -> "Inserted " <> shot (count table) <> " rows into table " <> tableName) $ 
  pythonTables dontCache (show chunkSize : fmap toS [dsn, tableName, fromSqlServerSchemaName schemaName])
  () (Left table) $ [q|
import pandas as pd
from sqlalchemy.engine import URL, create_engine

engine = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": sys.argv[2]})
# https://docs.sqlalchemy.org/en/20/glossary.html#term-executemany
# https://peps.python.org/pep-0249/#executemany
                      ,fast_executemany=True)

table = sys.argv[3]

# this runs in its own transaction:
# https://github.com/pandas-dev/pandas/blob/bd5ed2f6e2ef8713567b5c731a00e74d93caaf64/pandas/io/sql.py#L1022

# seems to returns always -1
inputDf.to_sql(table, engine, if_exists='append', index=False,
  chunksize=int(sys.argv[1]), schema=sys.argv[4])

outputDfs = []
|]

selectMssqlServerViaPandas :: (HasCallStack, Members '[Embed IO, GhcTime, Log, ErrorE, Python] r)
  => Text -> Text -> Sem r Table
selectMssqlServerViaPandas dsn query = measureLog Info
  (\t -> "Received " <> shot (count t) <> " rows") $ pythonTable dontCache (toS <$> [dsn, query]) ()
  (Right []) $ [q|
import pandas as pd
from sqlalchemy.engine import URL, create_engine
import sqlalchemy as sa

engine = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": sys.argv[1]})
                      ,fast_executemany=True)

with engine.connect() as conn:
  outputDf = pd.read_sql(sa.text(sys.argv[2]), conn)
|]
{-# INLINABLE selectMssqlServerViaPandas #-}
