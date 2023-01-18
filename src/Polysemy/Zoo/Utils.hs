{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveAnyClass #-}

module Polysemy.Zoo.Utils
  (module Polysemy.Zoo.Utils
  ,module Reexport
  )where
import           Codec.Archive.Zip as Z
import qualified Control.Lens as Lens
import qualified Control.Monad.Writer
import           Data.Aeson
import           Data.Aeson.Types (Parser)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.Map as M
import qualified Data.Vector as V
import qualified Data.Yaml as Y
import qualified Data.Yaml.Internal as Y
import qualified Dhall as D
import           Dhall hiding (embed, maybe, void)
import           Dhall.Marshal.Encode ()
import           Formatting.Formatters hiding (left)
import qualified Hoff as H
import qualified Polysemy.Writer as P
import           Polysemy.Zoo.Prelude as Y
import qualified Prelude as P
import           System.Entropy (getEntropy)
import           System.FilePath.Glob
import           System.INotify
import           System.INotify as Reexport (EventVariety(..))
import           System.Process
import qualified Text.Libyaml as LY


-- * error handling + writing

newtype TextError = TextError Text
  deriving (Generic)
  deriving newtype (Hashable, Eq, Ord)

instance Exception TextError

instance Show TextError where
  show (TextError t) = toS t

type ErrorMsg = Text
type ErrorMsgs = [Text]

type ErrorE = Error ErrorMsg
type WriterE = Writer ErrorMsgs

type TE r = Member ErrorE r
type TW r = Member WriterE r

embedCatchWith :: (HasCallStack, Members '[Embed IO, ErrorE] r) => (Text -> Text) -> IO a -> Sem r a
embedCatchWith msg = fromExceptionVia $ \e -> msg $ shot @SomeException e <> "\n"
  <> toS (prettyCallStack callStack)
{-# INLINE embedCatchWith #-}

embedCatch :: (HasCallStack, Members '[Embed IO, ErrorE] r) => IO a -> Sem r a
embedCatch = withFrozenCallStack $ embedCatchWith id
{-# INLINE embedCatch #-}

-- | because it catches WrappedExc thrown by errorToIOFinal
semCatchNotUseful :: Members '[Final IO, ErrorE] r => Sem r a -> Sem r a
semCatchNotUseful = fromExceptionSemVia $ \e -> shot @SomeException e

tellErrorP :: Member (Writer [e]) r => Either e b -> Sem r (Maybe b)
tellErrorP = either (\x -> Nothing <$ tell [x]) pure2
{-# INLINABLE tellErrorP #-}


tellErrors :: (Container f, Members '[Writer [e], Error e] r) => f (Sem r a) -> Sem r (f a)
tellErrors = mapTellErrors id
{-# INLINE tellErrors #-}

mapTellErrors :: (Container f, Members '[Writer [e], Error e] r) => (v -> Sem r a) -> f v -> Sem r (f a)
mapTellErrors f = fmap containerPartitionEithers . mapM (try . f)
  >=> \(errs, vals) -> vals <$ tell (toList errs)
{-# INLINABLE mapTellErrors #-}

mapTellErrors' :: (Container f, Member (Writer [e]) r) => (v -> Sem (Error e : r) a) -> f v -> Sem r (f a)
mapTellErrors' f = fmap containerPartitionEithers . mapM (runError . f)
  >=> \(errs, vals) -> vals <$ tell (toList errs)
{-# INLINABLE mapTellErrors' #-}

mapTellErrorsWith :: (Container f, Members '[Writer [e2], Error e] r)
  => (v -> (Sem r a, e -> e2)) -> f v -> Sem r (f a)
mapTellErrorsWith f = fmap containerPartitionEithers . mapM ((\(ac, mod) -> left mod <$> try ac) . f)
  >=> \(errs, vals) -> vals <$ tell (toList errs)
{-# INLINABLE mapTellErrorsWith #-}

collectErrors :: (Container f, TE r) => (v -> Sem r a) -> f v -> Sem r (f a)
collectErrors f = fmap (first toList . containerPartitionEithers) . mapM (try . f) 
  >=> \(errs, vals) -> vals <$ when (not $ null errs) (throw $ unlines2 errs)
{-# INLINABLE collectErrors #-}

collectErrors2 :: TE r => Sem r a -> Sem r b -> Sem r (a,b)
collectErrors2 a b = do
  aE <- try a
  bE <- try b
  case (aE, bE) of
    (Left ae, Left be) -> throw $ unlines2 [ae,be]
    _ -> fromEither $ (,) <$> aE <*> bE
{-# INLINABLE collectErrors2 #-}

mapTellErrors'' :: (Container f, Member (Writer [a]) r) => (v -> Either a b) -> f v -> Sem r (f b)
mapTellErrors'' f = (\(errs, vals) -> vals <$ tell (toList errs)) . containerPartitionWith f
{-# INLINABLE mapTellErrors'' #-}

logWriterMsgs :: (HasCallStack, Member Log r) => Severity -> Sem (Writer ErrorMsgs ': r) a -> Sem r a
logWriterMsgs sev a = do
  (errs, r) <- runWriterAssocR a
  r <$ mapM (withFrozenCallStack $ log sev) errs
{-# INLINABLE logWriterMsgs #-}

-- throwWriterMsgs :: TE r => (ErrorMsgs -> ErrorMsg) -> Sem (Writer ErrorMsgs ': r) a -> Sem r a
throwWriterMsgs :: (Monoid (t a), Foldable t, Member (Error e) r)
  => (t a -> e) -> Sem (Writer (t a) : r) b -> Sem r b
throwWriterMsgs f a = do
  (errs, r) <- runWriterAssocR a
  if null errs then pure r else throw $ f errs
{-# INLINABLE throwWriterMsgs #-}

fromEitherWith :: Member (Error g) r => (e -> g) -> Either e a -> Sem r a
fromEitherWith f = either (throw . f) pure
{-# INLINABLE fromEitherWith #-}

errorToIOFinalThrow :: Members '[Final IO] r => Sem (Error Text : r) a -> Sem r a
errorToIOFinalThrow s = embedFinal . either (throwIO . TextError) pure =<< errorToIOFinal s
{-# INLINABLE errorToIOFinalThrow #-}

prependMsg :: (Member (Writer (f e)) r, Functor f, Semigroup e) => e -> Sem r b -> Sem r b
prependMsg msg = censor $ fmap (msg <>)
{-# INLINABLE prependMsg #-}


-- fromListNoDupsBy :: forall t r v a . (TE r, Typeable a, Eq a, Hashable a, Show a, Foldable t) => (v -> a) -> t v -> Sem r (HM.HashMap a v)
-- fromListNoDupsBy f xs | length res == length xs = pure res
--                       | True                    = res <$ mapErrors g (HM.toList $ groupByHm f xs)
--   where g (_, (_:|[])) = pure ()
--         g (k, x) = throwError $ "Duplicate " <> showt (typeOf k) <> " (" <> showt (length x) <> " occurences)"
--         res = HM.fromList $ fmap (\x -> (f x, x)) $ toList xs

-- todo: check if already part of env variable
-- prependEnv :: String -> String -> IO ()
-- prependEnv name value = setEnv name . maybe value ((value <> ";") <>) =<< lookupEnv name



-- * misc

patternToPath :: Day -> FilePath -> FilePath
patternToPath d p = formatTime defaultTimeLocale p (toBaseDay d)
{-# INLINABLE patternToPath #-}

getSingleFileZip :: FilePath -- ^ zip file path
                 -> IO LByteString
getSingleFileZip path = do ar <- Z.toArchive <$> BL.readFile path
                           case Z.filesInArchive ar of
                             [entry]    -> getFileFromZip' path entry ar
                             x          -> ioError $ userError $ "Expected zip archive with single entry, got:\n" <> show x <>
                                           "\nin file: " <> path
{-# INLINABLE getSingleFileZip #-}

getFileFromZip' :: FilePath -- ^ zip file path
                -> FilePath -- ^ entry path
                -> Z.Archive
                -> IO LByteString
getFileFromZip' path entry archive = 
  maybeThrow (userError $ "Entry no found in archive " <> path) $ fmap Z.fromEntry $ Z.findEntryByPath entry archive
{-# INLINABLE getFileFromZip' #-}

getFileFromZip :: FilePath -- ^ zip file path
               -> FilePath -- ^ entry path
               -> IO LByteString
getFileFromZip path entry = BL.readFile path >>= getFileFromZip' path entry . Z.toArchive
{-# INLINABLE getFileFromZip #-}

-- * measure elapsed time

showDuration :: (ConvertText LText b, TimeUnit u) => u -> b
showDuration u = toS $ format (fixed 5 % "s") $ secondsFrac $ toNanos u
{-# INLINABLE showDuration #-}

measureLog' :: (HasCallStack, Members '[Log, GhcTime] r) => Severity -> Sem r a -> Sem r a
measureLog' sev sem = withFrozenCallStack $ measureLog sev (const "") sem
{-# INLINABLE measureLog' #-}

measureLog :: (HasCallStack, Members '[Log, GhcTime] r) => Severity -> (a -> Text) -> Sem r a -> Sem r a
measureLog sev msg ac = do (elapsed, r) <- measure ac
                           withFrozenCallStack (log sev) $ "(" <> showDuration elapsed <> ") " <> msg r
                           pure r
{-# INLINABLE measureLog #-}


-- * lookup
intersectWriteMissing :: (Ord k, Foldable f, TW r) => (k->Text) -> (b -> k) -> (a -> b -> c) -> Map k a -> f b -> Sem r (Map k c)
intersectWriteMissing toMsg getKey combine map keys' = found <$ tell (toMsg <$> M.keys (M.difference keys found))
  where found = M.intersectionWith combine map keys
        keys = M.fromList $ fmapToFst getKey $ toList keys'
{-# INLINABLE intersectWriteMissing #-}
        
lookupError'
  :: (TE r, Show b, FoldableWithIndex b t, Lens.Ixed (t a), Typeable b, Lens.Index (t a) ~ b) =>
     Int -> Text -> b -> t a -> Sem r (Lens.IxValue (t a))
lookupError' showKeys msg k = fromEither . lookupThrow' showKeys msg k
{-# INLINEABLE lookupError' #-}

lookupErrorNoKeys = lookupError' 0
{-# INLINE lookupErrorNoKeys #-}
lookupErrorAllKeys = lookupError' maxBound
{-# INLINE lookupErrorAllKeys #-}

dropAndLowerCaseNext :: Int -> [Char] -> [Char]
dropAndLowerCaseNext n = \case { [] -> []; h:t -> toLower h : t} . drop n
  
-- seqM :: Monad m => m b -> m b
-- seqM = chain pureM

-- -- | similar: Control.Exception.evaluate instead
-- pureM :: Applicative f => a -> f a
-- pureM x = seq x $ pure x


-- -- | evaluate to WHNF
-- evaluateEither :: HasCallStack => a -> Either Text a
-- evaluateEither a = unsafePerformIO $ E.catch (Right <$> E.evaluate a) $ \e -> pure $ Left $ shot (e :: SomeException) 

-- evaluateThrow :: (HasCallStack, TE r) => Text -> a -> Sem r a
-- evaluateThrow msg a = unsafePerformIO $ E.catch (pure <$> E.evaluate a) $ \e -> pure $ throw $ msg <> shot (e :: SomeException) 

-- evaluateLog :: forall a r . (HasCallStack, Monoid a, Member Log r) => Severity -> Text -> a -> Sem r a
-- evaluateLog sev msg a = unsafePerformIO $ E.catch (pure <$> E.evaluate a)
--   $ \e -> pure $ mempty <$ log sev (msg <> shot (e :: SomeException))

runH :: TE r => H.H a -> Sem r a
runH = fromEither . H.runHEither
{-# INLINABLE runH #-}

logH :: (Monoid a, Member Log r) => Severity -> Text -> H.H a -> Sem r a 
logH sev msg = H.runHWith (\e -> mempty <$ withFrozenCallStack (log sev) (msg <> shot e)) pure
{-# INLINABLE logH #-}

-- * Dhall and FromJSON for Read instances

newtype FromDhallRead a = FromDhallRead { unFromDhallRead :: a }

instance (Typeable a, Read a) => FromDhall (FromDhallRead a) where
  autoWith _ = Decoder {..}
    where expected = D.expected D.strictText
          extract v = fromMonadic $ do
            t <- toMonadic $ D.extract D.strictText v
            let err = toMonadic $ extractError $ readError t $ Proxy @a                 
            maybe err (pure . FromDhallRead) $ readMaybe t


readError :: Typeable a => Text -> Proxy a -> Text
readError t p = "invalid Read value for " <> shot (typeRep p) <> ": '" <> t <> "'"
{-# INLINABLE readError #-}

jsonRead :: forall a . (Typeable a, Read a) => Text -> Parser a
jsonRead t = maybe (fail $ toS $ readError t $ Proxy @a) pure $ readMaybe t
{-# INLINABLE jsonRead #-}

newtype PolysemyWriter t r a = PolysemyWriter { runWriterInPolysemy :: Sem r a }
  deriving newtype (Functor, Applicative, Monad)

instance (Monoid t, Member (Writer t) r) => MonadWriter t (PolysemyWriter t r) where
  tell = coerce tell
  {-# INLINE tell #-}
  listen = fmap swap . coerce listen
  {-# INLINE listen #-}
  pass = coerce P.pass . fmap swap
  {-# INLINE pass #-}


-- * common types

type ColName            = Text
type ColDeclaration     = Text

type TableName = Text

type Dsn = Text

newtype KeepTempTables          = KeepTempTables Bool deriving newtype IsBool

toHex :: ByteString -> Text
toHex = decodeUtf8 . B16.encode

randomHex :: Int -> IO Text
randomHex n = toHex <$> getEntropy n

showIns :: Show a => Inspector f -> f a -> Text
showIns ins = maybe "{GOT NOTHING FROM INSPECTOR2}" shot . inspect ins
{-# INLINABLE showIns #-}

forceT :: HasCallStack => f b -> Sem (WithTactics e f m r) b
forceT x = flip forceIns x =<< getInspectorT
{-# INLINABLE forceT #-}

forceIns :: (HasCallStack, Applicative m) => Inspector f -> f a -> m a
forceIns ins = maybe (P.error "{GOT NOTHING FROM INSPECTOR1}") pure . inspect ins
{-# INLINABLE forceIns #-}

runTSimpleForce :: (HasCallStack, Functor f) => m b -> Sem (WithTactics e f m r) b
runTSimpleForce x = forceT =<< runTSimple x
{-# INLINABLE runTSimpleForce #-}

  


newtype SqlServerSchemaName = SqlServerSchemaName { fromSqlServerSchemaName :: Text }
  deriving (Eq, Show, Generic)
  deriving newtype (FromDhall)


finallyP :: Member (Error e) r => Sem r a -> Sem r b -> Sem r a
finallyP a sequel = onExceptionP a sequel <* sequel
{-# INLINABLE finallyP #-}

onExceptionP :: Member (Error e) r => Sem r a -> Sem r b -> Sem r a
onExceptionP a sequel = catch a $ (sequel >>) . throw
{-# INLINABLE onExceptionP #-}

modError :: Member (Error e) r => (e -> e) -> Sem r a -> Sem r a
modError mod a = catch a $ throw . mod
{-# INLINABLE modError #-}


lowerSev :: Severity -> Severity
lowerSev = \case
  Trace -> Trace
  x -> pred x

deriving instance Generic Severity
deriving instance ToJSON Severity

class (Traversable f, Foldable f, Functor f) => Container f where
  containerUnzip              :: f (x,y) -> (f x, f y)
  containerZipWith3           :: (a -> b -> c -> d) -> f a -> f b -> f c -> f d
  containerPartitionEithers   :: f (Either x y) -> (f x, f y)
  containerPartitionWith      :: (a -> Either x y) -> f a -> (f x, f y)
  containerFilter             :: (a -> Bool) -> f a -> f a 
  containerAppend             :: f a -> f a -> f a
  default containerAppend :: Semigroup (f a) => f a -> f a -> f a
  containerAppend = (<>)
  {-# INLINE containerAppend #-}

instance Container Vector where
  containerUnzip              = V.unzip
  containerPartitionEithers   = V.partitionWith id
  containerFilter             = V.filter
  containerPartitionWith      = V.partitionWith
  containerZipWith3           = V.zipWith3
  {-# INLINE containerUnzip #-}
  {-# INLINE containerPartitionEithers #-}
  {-# INLINE containerFilter #-}
  {-# INLINE containerPartitionWith #-}

instance Container [] where
  containerUnzip              = unzip
  containerPartitionEithers   = partitionEithers
  containerPartitionWith    f = partitionEithers . fmap f
  containerFilter             = filter
  containerZipWith3           = zipWith3
  {-# INLINE containerUnzip #-}
  {-# INLINE containerPartitionEithers #-}
  {-# INLINE containerFilter #-}
  {-# INLINE containerPartitionWith #-}

-- devmain = runSequentiallyOnTreeModification Nothing
--      "/nix/store/x1cp4jj3r3w89xcbg35pfnryp2xz2xg9-fswatch-1.17.1/bin/fswatch" "/tmp/asd" $ print True

-- devmain = runSequentiallyOnChokidar (Just 10000)
--   "node"
--   "/nix/store/qm3vq4241kv8vznw7iyf2a215c0mv8xr-live-server-1.2.2/lib/node_modules/live-server/node_modules/chokidar"
--   "/tmp/asd"
  

-- /nix/store/cbk9adnx7rwxjkfrg24z8d99mrsh2vyv-sass-1.66.1/lib/node_modules/sass/node_modules/chokidar


-- | Bug in Chokidar: sometimes chmod -x or +x does not trigger a change
--
-- node -e 'require("/nix/store/cbk9adnx7rwxjkfrg24z8d99mrsh2vyv-sass-1.66.1/lib/node_modules/sass/node_modules/chokidar").watch("/tmp/asd", {ignoreInitial:true}).on("all", console.log)'
--
runSequentiallyOnChokidar :: Maybe Int -> FilePath -> FilePath -> FilePath -> IO a -> IO ()
runSequentiallyOnChokidar microsecondsM pathToNodeBin pathToChokidar target =
  runSequentiallyOnProcessStdoutLine "runSequentiallyOnChokidar"
  microsecondsM pathToNodeBin ["-e", toS $ decodeUtf8 script]
  where script =
          [qq|require({encode pathToChokidar}).watch({encode target}, \{ ignoreInitial: true \}).\
             on('all', (e,p) => console.log(e,p))|]
            :: ByteString

-- | does NOT fire on symlink file changes!
runSequentiallyOnTreeModificationFsWatch :: Maybe Int -> FilePath -> FilePath -> IO a -> IO ()
runSequentiallyOnTreeModificationFsWatch microsecondsM pathToFsWatchBin target =
  runSequentiallyOnProcessStdoutLine "runSequentiallyOnTreeModificationFsWatch"
  microsecondsM pathToFsWatchBin
  ["-L" --symlinks
  ,"-r" -- recursive
  ,"--event", "Created"
  ,"--event", "Updated"
  ,"--event", "Removed"
  ,"--event", "Renamed"
  ,"--event", "MovedFrom"
  ,"--event", "MovedTo"
  ,"-o" -- just one line per batch
  ,"-l" -- latency
  ,"0.1"
  ,target]


runSequentiallyOnProcessStdoutLine :: String -> Maybe Int -> FilePath -> [String] -> IO a -> IO ()
runSequentiallyOnProcessStdoutLine msg microsecondsM bin args action =
  withCreateProcess (proc bin args) { std_out = CreatePipe } $ \_ stdoutH _ ph ->
  race_ (H.expectH stdoutH $ flip (runSequentiallyOnGetLine microsecondsM) action)
  $ waitForProcess ph >>= \case
  ExitSuccess -> pass
  e@(ExitFailure _) -> throwIO $ userError $ msg <> ": " <> show e


runSequentiallyOnGetLine :: Maybe Int
  -- ^ Wait given number of microseconds after first event in case there are more events firing in short
  -- succession
  -> Handle -> IO a -> IO ()
runSequentiallyOnGetLine microsecondsM handle action = do
  mv <- newEmptyMVar
  race_ (catchIOError (forever $ BS.hGetLine handle >> tryPutMVar mv ())
         $ \e -> if isEOFError e then pass else ioError e)
    $ forever $
    takeMVar mv >> maybe id (\ms -> (threadDelay ms >> tryTakeMVar mv >>)) microsecondsM action

-- | not very useful to watch file system trees: does not follow symlinks and is not recursive
runSequentiallyOnFileChange :: Bool -- ^ Log events to stdout
  -> [EventVariety]
  -> [String] -- ^ include glob patterns (`[]` is the same as `["**/*"]`)
  -> [String] -- ^ exclude glob patterns
  -> [ByteString] -- ^ paths to watch (recursively)
  -> Int
  -- ^ Wait given number of microseconds after first event in case there are more events firing in short
  -- succession
  -> IO a -> IO ()
runSequentiallyOnFileChange logEvents eventVariety include exclude paths microseconds action = withINotify
  $ \inotify -> do
  mv <- newEmptyMVar
  let handleEvent e = do
        let match = maybe True testPath $ eventFilePath e
        when logEvents $ putStrLn $
          "Match: " <> show match <> ", Event:\n" <> show e
        when match $ void (tryPutMVar mv ())
      testPath f = anyMatch id includePs && anyMatch not excludePs
        where anyMatch mod = \case {[] -> True; x -> mod $ any (\p -> match p f) x}
      (includePs, excludePs) = (compile <$> include, compile <$> exclude)
  forM paths $ \p -> addWatch inotify eventVariety p handleEvent
  forever $ takeMVar mv >> threadDelay microseconds >> tryTakeMVar mv >> action

-- devWatch = runOnFileChange True [Move,Create,CloseWrite,Delete]
           -- [] [] ["/tmp/asd"] 100000 $ print True

eventFilePath :: Event -> Maybe FilePath
eventFilePath = fmap (toS . fromUtf8Lenient) . \case
  Accessed _ p          -> p
  Modified _ p          -> p
  Attributes _ p        -> p
  Closed _ p _          -> p
  Opened _ p            -> p
  MovedOut _ p _        -> Just p
  MovedIn _ p _         -> Just p
  Created _ p           -> Just p
  Deleted _ p           -> Just p
  MovedSelf _           -> Nothing
  DeletedSelf           -> Nothing
  Unmounted             -> Nothing
  QOverflow             -> Nothing
  Ignored               -> Nothing
  Unknown _             -> Nothing

liftYamlDecode :: forall c r . (HasCallStack, TE r, Member (Embed IO) r,  TW r, FromJSON c)
  => ByteString -> Sem r c
liftYamlDecode path = do
  (w,r) <- fromEitherWith shot <=< embedCatch $ (Y.decodeHelper_ . LY.decode) path
  r <$ tell (shot <$> w)

-- | Will throw error on DuplicateKey-warning
liftYamlDecodeFile :: forall c r . (HasCallStack, Member (Embed IO) r,  TE r, TW r, FromJSON c)
  => FilePath -> Sem r c
liftYamlDecodeFile path = do
  (w,r) <- fromEitherWith shot <=< embed $ Y.decodeFileWithWarnings path
  r <$ tell (shot <$> w)

mFromListTellDupsSem :: forall a t v r. (TW r, Typeable a, Ord a, Show a, Foldable t)
  => t (a,v) -> Sem r (M.Map a v)
mFromListTellDupsSem = fmap (runWriterInPolysemy @ErrorMsgs) mFromListTellDups
{-# INLINABLE mFromListTellDupsSem #-}

mFromListByTellDupsSem :: forall a t v r. (TW r, Functor t, Typeable a, Ord a, Show a, Foldable t)
  => (v -> a) -> t v -> Sem r (M.Map a v)
mFromListByTellDupsSem = fmap2 (runWriterInPolysemy @ErrorMsgs) mFromListByTellDups
{-# INLINABLE mFromListByTellDupsSem #-}

hmFromListTellDupsSem :: forall a t v r. (TW r, Typeable a, Hashable a, Show a, Foldable t)
  => t (a,v) -> Sem r (HM.HashMap a v)
hmFromListTellDupsSem = fmap (runWriterInPolysemy @ErrorMsgs) hmFromListTellDups
{-# INLINABLE hmFromListTellDupsSem #-}

hmFromListByTellDupsSem :: forall a t v r. (TW r, Functor t, Typeable a, Hashable a, Show a, Foldable t)
  => (v -> a) -> t v -> Sem r (HM.HashMap a v)
hmFromListByTellDupsSem = fmap2 (runWriterInPolysemy @ErrorMsgs) hmFromListByTellDups
{-# INLINABLE hmFromListByTellDupsSem #-}

memoizeForeignStoreIndex :: Word32
memoizeForeignStoreIndex = 333

fsCacheForeignStoreIndex :: Word32
fsCacheForeignStoreIndex = 334

tell1 :: (Applicative f, Member (Writer (f a)) r) => a -> Sem r ()
tell1 = tell . pure
{-# INLINE tell1 #-}

today' :: Member (Embed IO) r => Sem r Day
today' = embed today
{-# INLINE today' #-}

now' :: Member (Embed IO) r => Sem r Time
now' = embed now
{-# INLINE now' #-}
