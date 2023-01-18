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
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Yaml as Y
import qualified Dhall as D
import           Dhall hiding (embed, maybe, extract, void)
import           Dhall.Marshal.Encode ()
import           Formatting.Formatters
import qualified Hoff as H
import qualified Polysemy.Writer as P
import           Polysemy.Zoo.Prelude
import qualified Prelude as P
import           System.Entropy (getEntropy)
import           System.FilePath.Glob
import           System.INotify
import           System.INotify as Reexport (EventVariety(..))


-- * error handling + writing

newtype TextError = TextError Text
                  deriving (Generic)

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

tellErrorP :: TW r => Either ErrorMsg b -> Sem r (Maybe b)
tellErrorP = either (\x -> Nothing <$ tell [x]) pure2
{-# INLINABLE tellErrorP #-}


tellErrors :: (Traversable f, Container f, TW r, TE r) => f (Sem r a) -> Sem r (f a)
tellErrors = mapTellErrors id
{-# INLINE tellErrors #-}

mapTellErrors :: (Traversable f, Container f, TW r, TE r) => (v -> Sem r a) -> f v -> Sem r (f a)
mapTellErrors f = fmap containerPartitionEithers . mapM (try . f)
  >=> \(errs, vals) -> vals <$ tell (toList errs)
{-# INLINABLE mapTellErrors #-}

mapTellErrorsWith :: (Traversable f, Container f, TW r, TE r)
  => (v -> (Sem r a, ErrorMsg -> ErrorMsg)) -> f v -> Sem r (f a)
mapTellErrorsWith f = fmap containerPartitionEithers . mapM ((\(ac, mod) -> (_Left %~ mod) <$> try ac) . f)
  >=> \(errs, vals) -> vals <$ tell (toList errs)
{-# INLINABLE mapTellErrorsWith #-}



collectErrors :: (Traversable f, Container f, TE r) => (v -> Sem r a) -> f v -> Sem r (f a)
collectErrors f = fmap (first toList . containerPartitionEithers) . mapM (try . f) 
  >=> \(errs, vals) -> vals <$ when (not $ null errs) (throw $ T.unlines errs)
{-# INLINABLE collectErrors #-}


collectErrors2 :: TE r => Sem r a -> Sem r b -> Sem r (a,b)
collectErrors2 a b = do
  aE <- try a
  bE <- try b
  case (aE, bE) of
    (Left ae, Left be) -> throw $ unlines [ae,be]
    _ -> fromEither $ (,) <$> aE <*> bE
{-# INLINABLE collectErrors2 #-}

mapTellErrors' :: (Foldable f, Container f, Member (Writer [a]) r) => (v -> Either a b) -> f v -> Sem r (f b)
mapTellErrors' f = (\(errs, vals) -> vals <$ tell (toList errs)) . containerPartitionWith f
{-# INLINABLE mapTellErrors' #-}

logWriterMsgs :: (HasCallStack, Member Log r) => Severity -> Sem (Writer ErrorMsgs ': r) a -> Sem r a
logWriterMsgs sev a = do
  (errs, r) <- runWriterAssocR a
  r <$ mapM (log sev) errs
{-# INLINABLE logWriterMsgs #-}

throwWriterMsgs :: TE r => (ErrorMsgs -> ErrorMsg) -> Sem (Writer ErrorMsgs ': r) a -> Sem r a
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

prependMsg :: TW r => ErrorMsg -> Sem r a -> Sem r a
prependMsg msg = censor $ fmap (msg <>)
{-# INLINABLE prependMsg #-}

-- * duplicates

groupByHm :: (Eq k, Hashable k, Semigroup (f a), Applicative f, Foldable t) => t (k,a) -> HM.HashMap k (f a)
groupByHm = HM.fromListWith (<>) . fmap (second pure) . toList
{-# INLINABLE groupByHm #-}

fromListByTellDups :: forall a t v r. (TW r, Typeable a, Eq a, Hashable a, Show a, Foldable t)
  => t (a,v) -> Sem r (HM.HashMap a v)
fromListByTellDups xs = HM.traverseWithKey g $ (groupByHm xs :: HM.HashMap a (NonEmpty v))
  where g _ (x:|[])     = pure x
        g k (x:|r)      = x <$ tell ["Key (" <> showt (typeOf k) <> ") " <> shot k <> " appears " <> showt (succ $ length r) <> " times" :: Text]
        g :: a -> NonEmpty v -> Sem r v
{-# INLINABLE fromListByTellDups #-}

extract2 :: Functor f => (t -> b) -> f t -> f (t, b)
extract2 f = fmap $ \x -> (x, f x)
{-# INLINABLE extract2 #-}

extract :: Functor f => (t -> b) -> f t -> f (b, t)
extract f = fmap $ \x -> (f x, x)
{-# INLINABLE extract #-}

fromListByTellDups' :: forall a t v r. (TW r, Functor t, Typeable a, Eq a, Hashable a, Show a, Foldable t)
  => (v -> a) -> t v -> Sem r (HM.HashMap a v)
fromListByTellDups' f = fromListByTellDups . extract f
{-# INLINABLE fromListByTellDups' #-}

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
measureLog sev msg ac = withFrozenCallStack $ do (elapsed, r) <- measure ac
                                                 slap r $ log sev $ "(" <> showDuration elapsed <> ") " <> msg r
{-# INLINABLE measureLog #-}


-- * error  handling using Prelude.error (rethink this!)
eitherErrorWithM :: (HasCallStack, Applicative m,  ConvertText s String) => (e -> s) -> Either e a -> m a
eitherErrorWithM f = withFrozenCallStack $ either (P.error . toS . f) pure
{-# INLINABLE eitherErrorWithM #-}

eitherErrorShowM :: (HasCallStack, Applicative m,  Show s) => Either s a -> m a
eitherErrorShowM = withFrozenCallStack $ eitherErrorWithM show
{-# INLINABLE eitherErrorShowM #-}

eitherErrorM :: (HasCallStack, Applicative m,  ConvertText s String) => Either s a -> m a
eitherErrorM = withFrozenCallStack $ eitherErrorWithM id
{-# INLINABLE eitherErrorM #-}

-- * lookup
intersectWriteMissing :: (Ord k, Foldable f, TW r) => (k->Text) -> (b -> k) -> (a -> b -> c) -> Map k a -> f b -> Sem r (Map k c)
intersectWriteMissing toMsg getKey combine map keys' = found <$ tell (toMsg <$> M.keys (M.difference keys found))
  where found = M.intersectionWith combine map keys
        keys = M.fromList $ extract getKey $ toList keys'
{-# INLINABLE intersectWriteMissing #-}
        
lookupError'
  :: (TE r, Show b, FoldableWithIndex b t, Lens.Ixed (t a), Typeable b, Lens.Index (t a) ~ b) =>
     Bool -> Text -> b -> t a -> Sem r (Lens.IxValue (t a))
lookupError' showKeys msg k c = note (toS $ err <> toS msg) $ c Lens.^? Lens.ix k
  where err = shot (typeOf k) <> " not found: " <> shot k <> ". " <> asd
        asd | showKeys  = "Available:\n" <> unlines (shot <$> (c ^.. Lens.ifolded . Lens.asIndex))
            | True      = "Container count " <> shot (length c)
{-# INLINEABLE lookupError' #-}

lookupErrorNoKeys = lookupError' False
{-# INLINE lookupErrorNoKeys #-}
lookupErrorKeys = lookupError' True
{-# INLINE lookupErrorKeys #-}

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
logH sev msg = H.runHWith (\e -> mempty <$ log sev (msg <> shot e)) pure
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

class Container f where
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

checkAbsError :: TE r => ErrorMsg -> Double -> Double -> Double -> Sem r ()
checkAbsError msg tol expected actual = when (abs (actual - expected) > tol)
  $ throw $ T.concat [msg, "abs(", showt actual, " - ", showt expected, ") > ", showt tol]
{-# INLINABLE checkAbsError #-}


checkRelError :: TE r => ErrorMsg -> Double -> Double -> Double -> Sem r ()
checkRelError msg tol expected actual = when (abs (actual - expected) > tol * sum)
  $ throw $ T.concat [msg, "abs(", showt actual, " - ", showt expected, ") > ", showt tol, " * ", showt sum]
  where sum = (actual + expected) * 0.5
{-# INLINABLE checkRelError #-}




-- | Wait given number of microseconds after first event in case there are more events firing in short
-- succession
runOnFileChange :: Bool -- ^ Log events to stdout
  -> [EventVariety]
  -> [String] -- ^ include glob patterns
  -> [String] -- ^ exclude glob patterns
  -> [ByteString] -> Int -> IO a -> IO ()
runOnFileChange logEvents eventVariety include exclude paths microseconds action = withINotify
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

-- | Will throw error on DuplicateKey-warning
liftYamlDecode :: (Member (Embed IO) r,  TW r, FromJSON c) => FilePath -> Sem r c
liftYamlDecode path = do (w,r) <- eitherErrorShowM <=< embed $ Y.decodeFileWithWarnings path
                         r <$ tell (shot <$> w)

