{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fplugin=Data.Record.Anon.Plugin #-}

module Polysemy.Zoo.Python
  (module Polysemy.Zoo.Python
  ,module Reexport) where

import           Hoff
import           Hoff as Reexport
  (CommandLineArgs
  , PythonProcess(..)
  , basicPp
  , handleLinesWith
  , pSource_
  , pArgs_
  , pInput_
  , pEnv_
  , pStdout_
  , pStderr_)
import qualified Hoff.Examples as HE
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
  -- | see documentation of Hoff.Python.pythonEvalByteString
  PythonByteString      :: HasCallStack =>
    (Maybe (KeyDerivation (Maybe ByteString))) -> RawPythonProcess -> Python m ByteString

  -- | see documentation of Hoff.Python.pythonEvalHoff
  PythonTable           :: (ToJSON j, HasCallStack) =>
    (Maybe (KeyDerivation j))
    -> PythonProcess j -> Either Table [(Text,Table)] -> Python m Table

  -- | see documentation of Hoff.Python.pythonEvalHoffs
  PythonTables          :: (ToJSON j, HasCallStack) =>
    (Maybe (KeyDerivation j))
    -> PythonProcess j -> Either Table [(Text,Table)] -> Python m [(Text, Table)]

data PythonConfig =  PythonConfig
  { cPythonBin                  :: FilePath
  , cPythonPath                 :: Maybe String
  , cPythonProcessModifier      :: forall j . PythonProcess j -> PythonProcess j
  }

type KeyDerivation j = (PythonProcess j, Maybe ByteString -- ^ this is the processes' raw stdin
                       ) -> FsCacheKey

makeSem ''Python

-- | derives key from pSource, pArgs, pInput, pColumnNames
--
-- before applying `cPythonProcessModifier`
keyVariant1 :: FsCacheBucket -> Maybe (KeyDerivation j)
keyVariant1 bucket = Just $ (bucket,) . md5init . kv1

kv1 (PythonProcess{..}, input) = (pSource, (pArgs, (fromMaybe "" input, show pColumnNames)))

-- | Like `keyVariant1` plus `pEnv` (before cPythonPath is set), 
keyVariant2 :: FsCacheBucket -> Maybe (KeyDerivation j)
keyVariant2 bucket = Just $ \pp -> let env = concatMap (\(a,b) -> [a,b]) $ concat $ pEnv $ fst pp
  in (bucket, md5init $ (kv1 pp, env))

instance Encodable (Either Text [(Text, Table)]) where
  encodeWithError = fmap encode . traverse3 ensureEncodable

instance Decodable (Either Text [(Text, Table)]) where
  decode' = fmap4 fromDecodeOnly decode

instance Encodable (Either Text Table) where
  encodeWithError = fmap encode . traverse ensureEncodable

instance Decodable (Either Text Table) where
  decode' = fmap2 fromDecodeOnly decode

runPythonInEmbedIO :: forall a r . HasPythonEffects r => Sem (Python : r) a -> Sem r a
runPythonInEmbedIO = interpret $ \case
  PythonByteString      toKey pp -> embedPython fsCachedSerialised pp
    $ \f pp2 -> (($ (pp, pInput pp)) <$> toKey, pythonEvalByteString f pp2)
  PythonTable           toKey pp tables -> do
    soTables <- toSo tables
    fromEither <=< embedPython fsCachedSerialisable pp
      $ \f pp2 -> first (\input -> toKey <&> ($ (pp,input))) $ pythonEvalHoff f pp2 soTables
  PythonTables          toKey pp tables   -> do
    soTables <- toSo tables
    fromEither <=< embedPython fsCachedSerialisable pp
      $ \f pp2 -> first (\input -> toKey <&> ($ (pp,input))) $ pythonEvalHoffs f pp2 soTables
  where toSo = let es = fromEither . ensureEncodable in _Left es <=< _Right (traverse2 es)
        toSo :: Either Table [(Text,Table)] -> Sem r (Either EoTable [(Text, EoTable)])
{-# INLINABLE runPythonInEmbedIO #-}

type HasPythonEffects r = Members '[Embed IO, ErrorE, FilesystemCache, Reader PythonConfig] r

embedPython :: (Typeable a, HasCallStack, HasPythonEffects r) =>
  (FsCacheKey -> Sem r a -> Sem r a) -> PythonProcess j
  -> (FilePath -> PythonProcess j -> (Maybe FsCacheKey, IO a)) -> Sem r a
embedPython cache pp getRunner = do
  PythonConfig{..} <- ask
  let ppEnv = "PYTHONPATH"
  setPythonPathEnv <- forMaybe (pure id) cPythonPath $ \pythonPath -> do
    env <- embed getEnvironment
    pure $ pEnv_ %~  Just . ((ppEnv, pythonPath):) . fromMaybe (P.filter (\(n,_) -> n /= ppEnv) env) 
  let (key, run) = getRunner cPythonBin $ cPythonProcessModifier $ setPythonPathEnv pp
  maybe id cache key $ embedCatch run
{-# INLINABLE embedPython #-}


runPythonInEmbedIONoCache :: Members [Final IO, Embed IO, ErrorE, Log, GhcTime] r => PythonConfig
  -> Sem (Python : FilesystemCache : Reader PythonConfig : r) a -> Sem r a
runPythonInEmbedIONoCache config = runReader config
  . runFilesystemCache (FsCacheConfig Nothing False "/unused_cache_folder" mempty) . runPythonInEmbedIO
{-# INLINABLE runPythonInEmbedIONoCache #-}

runPythonInIONoCache :: PythonConfig ->
  Sem '[Python, FilesystemCache, Reader PythonConfig, Log , ErrorE , GhcTime, Embed IO, Final IO] a -> IO a
runPythonInIONoCache config = runFinal . embedToFinal . interpretTimeGhc . errorToIOFinalThrow
  . interpretLogStderr . runPythonInEmbedIONoCache config
{-# INLINABLE runPythonInIONoCache #-}



insertMssqlServerViaPandas :: (HasCallStack, Members '[Embed IO, GhcTime, Log, ErrorE, Python] r)
  => Word32 -> Text -> TableName -> SqlServerSchemaName -> Table -> Sem r ()
{-# INLINABLE insertMssqlServerViaPandas #-}
insertMssqlServerViaPandas chunkSize dsn tableName schemaName table = void $ do
  binaryCols <- runH $ execW @Text #c (isSuffixOf " ByteString" <$> #t) $ meta table
  let args = ANON { chunkSize = chunkSize, dsn = dsn, table = tableName
                  , schema = fromSqlServerSchemaName schemaName, binaryCols = binaryCols }
  measureLog Debug (\_ -> "Inserted " <> shot (count table) <> " rows into table " <> tableName) $ 
    pythonTables Nothing (originalColNames $ basicPp source args) $ Left table
  where source = [q|
import pandas as pd
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.types import (LargeBinary)


# print(jsonArg)

engine = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": jsonArg['dsn']})
# https://docs.sqlalchemy.org/en/20/glossary.html#term-executemany
# https://peps.python.org/pep-0249/#executemany
                      ,fast_executemany=True)

# this runs in its own transaction:
# https://github.com/pandas-dev/pandas/blob/bd5ed2f6e2ef8713567b5c731a00e74d93caaf64/pandas/io/sql.py#L1022


# seems to returns always -1
try:
  inputDf.to_sql(jsonArg['table'], engine, if_exists='append', index=False,
    chunksize=int(jsonArg['chunkSize']), schema=jsonArg['schema'],
    dtype = {x: LargeBinary() for x in jsonArg['binaryCols']})
except Exception:
  raise Exception(inputDf.dtypes)

outputDfs = []
|]

selectMssqlServerViaPandas :: (HasCallStack, Members '[Embed IO, GhcTime, Log, ErrorE, Python] r)
  => ColumnNameConvert -> Text -> Text -> Sem r Table
selectMssqlServerViaPandas cnc dsn query = measureLog Info
  (\t -> "Received " <> shot (count t) <> " rows") $ pythonTable Nothing (basicPp source ())
  { pColumnNames = cnc, pArgs = toS <$> [dsn, query] } $ Right []
  where source = [q|
import pandas as pd
from sqlalchemy.engine import URL, create_engine
import sqlalchemy as sa

engine = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": sys.argv[1]})
                      ,fast_executemany=True)

with engine.connect() as conn:
  outputDf = pd.read_sql(sa.text(sys.argv[2]), conn)
|]
{-# INLINABLE selectMssqlServerViaPandas #-}


failNonUniqueRenamig = runPythonInIONoCache (PythonConfig "python" Nothing id) $
  pythonTable Nothing (basicPp "outputDf = pd.DataFrame({'a_b':[1,2], 'a_B':[3,4]})" ()) (Left HE.t1)


devmain = runPythonInIONoCache (PythonConfig "python" Nothing id) $
  try $ pythonTables Nothing (basicPp "outputDfs = []" ())
  $ Left $ unsafeH $ update [ci #col (TextError "asd")] $ HE.t1
