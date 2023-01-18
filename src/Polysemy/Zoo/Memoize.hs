{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveAnyClass #-}

-- *
--
-- Source location and type part of the internal key used for looking up the value, allowing for example this:
--
-- memptyCached :: (Typeable a, Member ErrorE r, Member Memoize r, Monoid a) => Sem r a
-- memptyCached = runMemoized Info [] $ pure mempty
--
-- pureDefCache :: (Typeable f, Typeable a, Member ErrorE r, Member Memoize r, Applicative f, Default a) =>
--      Sem r (f a)
-- pureDefCache = runMemoized Info [] $ pure2 def
--
module Polysemy.Zoo.Memoize where



import qualified Data.ByteString as B
import qualified Data.Cache as C
import           Data.Serialize.Text ()
import qualified Data.Text as T
import           Data.Vector.Serialize ()
import qualified Foreign.Store as FS
import           GHC.Stack (SrcLoc(..))
import           Polysemy.AtomicState
import qualified Polysemy.Cache as PC
import           Polysemy.Log hiding (trace)
import           Polysemy.Zoo.Prelude hiding (SrcLoc)
import           Polysemy.Zoo.Utils
import qualified Prelude as P
import qualified Type.Reflection as R


type CacheKey                   = (MemoizeKey, TypeRep, MinimalSrcLoc)
type MemoizeCache               = C.Cache CacheKey Any
type MemoizeCacheEffect         = PC.Cache CacheKey Any
type MemoizeCacheStateEffect    = AtomicState MemoizeCache
type MinimalSrcLoc              = (FilePath, Int, Int)

data MemoizeKey = forall a . (Eq a, Typeable a, Hashable a, Show a) => MemoizeKey a

instance Show MemoizeKey where
  show (MemoizeKey a) = show a

instance Eq MemoizeKey where
  (MemoizeKey a) == (MemoizeKey b) | Just R.HRefl <- R.typeOf a `R.eqTypeRep` R.typeOf b = a == b
                                   | True = False

instance Hashable MemoizeKey where
  hashWithSalt salt = withKey $ hashWithSalt salt

withKey :: (forall a . (Hashable a, Show a) => a -> b) -> MemoizeKey -> b
withKey f (MemoizeKey a) = f a

type MemoizeMultiContainer f = (Container f, Traversable f)

data Memoize m a where
  UnsafeMemoize         :: (HasCallStack, Typeable a)
    => Bool -- ^ Ignore cache value and recalculate
    -> Severity -> Maybe Text -- ^ message to be logged instead of key
    -> SrcLoc -> MemoizeKey -> m a -> Memoize m a
  UnsafeMemoizeMulti    :: (HasCallStack, MemoizeMultiContainer f, Typeable a)
    => Bool -- ^ Ignore cache values and recalculate
    ->  Severity  -> Maybe Text
    -- ^ message to be logged instead of key
    -> SrcLoc
    -> (b -> MemoizeKey) -> f b -> w -- ^ default w, if there is no recalc needed
    -> (f b -> m (w, f (b, a)))
    -- ^ this function gets passed the cache misses and is expected to return a subset. any returned value
    -- will be inserted into the cache.
    -- 
    -- `w` will be passed through
    --
    -- Cache misses that are not returned will not be inserted. if a future retry of those misses is not
    -- wished, one can use `Maybe a` and use Nothing to indicate that the fact that there is no value
    -- for this key should be cached.
    -> Memoize m (w, f (b,a))
    -- ^ the result is a combination the following two containers:
    --  (1) cache hits
    --  (2) the calculated results

makeSem ''Memoize

-- * Interpreters

runMemoizeInCacheEffect :: forall a r . (HasCallStack, Members '[MemoizeCacheEffect, Log, GhcTime] r)
  => Sem (Memoize : r) a -> Sem r a
runMemoizeInCacheEffect = interpretH toTactical
{-# INLINABLE runMemoizeInCacheEffect #-}

toTactical :: forall m r b (rInitial :: EffectRow)
  . (HasCallStack, Members '[MemoizeCacheEffect, Log, GhcTime] r)
  => Memoize (Sem rInitial) b -> Tactical m (Sem rInitial) r b
toTactical = \case
  UnsafeMemoizeMulti ignoreCache sev msg loc defW keyF inputs action
    -> multi ignoreCache sev msg loc defW keyF inputs action
  UnsafeMemoize ignoreCache sev msg loc key action ->
    bool (PC.lookup fullKey >>= maybe (miss "miss") hit) (miss "forced recalc") ignoreCache
    where fullKey@(_, tr, _) = getFullKey (Proxy @b) loc key
          showKey = mconcat [": ", fromMaybe (shot key) msg, ", Type: ", shot tr]
          hit entry = (log (lowerSev sev) $ "Memoize cache hit" <> showKey) >> pureT (unsafeCoerce entry)
          miss msg = measureLog sev (const $ "Memoize cache " <> msg <> " " <> showKey) $ do
            r <- runTSimpleForce action
            pureT r <* seq r (PC.insert fullKey $ unsafeCoerce r)
{-# INLINABLE toTactical #-}

multi :: forall f b a m r (rInitial :: EffectRow) w .
  (HasCallStack, MemoizeMultiContainer f, Typeable a, Members '[MemoizeCacheEffect, Log, GhcTime] r)
  => Bool -> Severity -> Maybe Text -> SrcLoc -> (b -> MemoizeKey) -> f b ->
  w -> (f b -> Sem rInitial (w, f (b, a))) -> Tactical m (Sem rInitial) r (w, f (b, a))
multi ignoreCache sev msgM loc keyF inputs defW action = do
  (recalc, (hits, hitKeys)) <- fmap (second containerUnzip . containerPartitionEithers)
    $ forM inputs $ \i ->
    let k = keyF i
        fk = getFk k in maybe (Left (i, k)) (Right . (,k) . (i,) . unsafeCoerce) <$>
                        bool (pure Nothing) (PC.lookup fk) (not ignoreCache)
  log (lowerSev sev) $ msg "hits" hitKeys
  
  if null recalc then pureT (defW, hits) else do
    (w, res) <- fmap (second $ fmapToSnd $ keyF . fst) $ measureLog sev
      (const $ msg (bool "recalculated misses" "forced recalcs" ignoreCache) (snd <$> recalc))
      $ runTSimpleForce $ action $ fst <$> recalc 
    pureT . ((w,) . containerAppend hits) =<<
      forM res (\((i,r), k) -> (i,r) <$ seq r (PC.insert (getFk k) $ unsafeCoerce r))

  where getFk = getFullKey (Proxy @a) loc
        msg x keys = "Memoize multi cache for type " <> shot (typeRep $ Proxy @a) <> ", " <> x <> ": " <>
          showt (length keys) <> fromMaybe shlist msgM
          where shlist = case toList keys of
                  [] -> ""
                  y -> "\n" <> T.intercalate "\n" (shot <$> y)
{-# INLINABLE multi #-}

getFullKey :: Typeable a => Proxy a -> SrcLoc -> MemoizeKey -> CacheKey
getFullKey p loc key = (key, typeRep p, (srcLocFile loc, srcLocStartLine loc, srcLocStartCol loc))
{-# INLINABLE getFullKey #-}

runMemoizeCacheEffect :: (Member (Final IO) r, Member (Embed IO) r) =>
      Maybe (IORef MemoizeCache) -> Sem (MemoizeCacheEffect : MemoizeCacheStateEffect : r) b -> Sem r b
runMemoizeCacheEffect cacheRefM = bind (maybe (embedFinal newCache) pure cacheRefM)
                               . flip runAtomicStateIORef . PC.runCacheAtomicState
{-# INLINABLE runMemoizeCacheEffect #-}

runMemoize :: (HasCallStack, Members '[Final IO, Embed IO, Log, GhcTime] r)
  => Maybe (IORef MemoizeCache) -> 
  Sem (Memoize : MemoizeCacheEffect : MemoizeCacheStateEffect : r) a -> Sem r a
runMemoize cacheRefM = runMemoizeCacheEffect cacheRefM . runMemoizeInCacheEffect
{-# INLINABLE runMemoize #-}

runMemoizeInIO :: Sem '[Memoize, MemoizeCacheEffect, MemoizeCacheStateEffect, Log
                       , Error Text, GhcTime, Embed IO, Final IO] a -> IO a
runMemoizeInIO = runFinal . embedToFinal . interpretTimeGhc
  . errorToIOFinalThrow . interpretLogStderr . runMemoize Nothing
{-# INLINABLE runMemoizeInIO #-}

runMemoizedForeignStore :: Members '[Final IO, Embed IO, Log, GhcTime] r
                        => Sem (Memoize : MemoizeCacheEffect : MemoizeCacheStateEffect : r) a -> Sem r a
runMemoizedForeignStore ac = do ref <- embedFinal $ recoverCacheFromStore memoizeForeignStoreIndex
                                runMemoize (Just ref) ac
{-# INLINABLE runMemoizedForeignStore #-}

runMemoizedForeignStoreInIO :: Sem '[Memoize, MemoizeCacheEffect, MemoizeCacheStateEffect, Log
                       , Error Text, GhcTime, Embed IO, Final IO] a -> IO a
runMemoizedForeignStoreInIO = runFinal . embedToFinal . interpretTimeGhc
  . errorToIOFinalThrow . interpretLogStderr . runMemoizedForeignStore
{-# INLINABLE runMemoizedForeignStoreInIO #-}


-- * Actions

-- | see UnsafeMemoize for documentation
runMemoized :: (TE r, HasCallStack, Typeable a, Members '[Memoize] r)
  => Severity -> Maybe Text -> MemoizeKey -> Sem r a -> Sem r a
runMemoized sev msg k a = withFrozenCallStack
  -- this combination of `withFrozenCallStack` and `withCallStackLoc` is important for this function to work correctly
  $ withCallStackLoc $ \loc -> unsafeMemoize False sev msg loc k a
{-# INLINABLE runMemoized #-}

-- | see UnsafeMemoize for documentation
runMemoized2 :: (TE r, HasCallStack, Typeable a, Members '[Memoize] r)
  => Bool -> Severity -> Maybe Text -> MemoizeKey -> Sem r a -> Sem r a
runMemoized2 ignoreCache sev msg k a = withFrozenCallStack
  -- this combination of `withFrozenCallStack` and `withCallStackLoc` is important for this function to work correctly
  $ withCallStackLoc $ \loc -> unsafeMemoize ignoreCache sev msg loc k a
{-# INLINABLE runMemoized2 #-}

-- | see UnsafeMemoizeMulti for documentation
runMemoizedMultiContext :: (HasCallStack, MemoizeMultiContainer f, Members '[ErrorE, Memoize] r, Typeable a) =>
  Bool -> Severity -> Maybe Text -> (b -> MemoizeKey) -> f b -> w -> (f b -> Sem r (w, f (b, a)))
  -> Sem r (w, f (b, a))
runMemoizedMultiContext ignoreCache log msg f k defW a = withFrozenCallStack
  -- this combination of `withFrozenCallStack` and `withCallStackLoc` is important for this function to work correctly
  $ withCallStackLoc
  $ \loc -> unsafeMemoizeMulti ignoreCache log msg loc f k defW a
{-# INLINABLE runMemoizedMultiContext #-}

runMemoizedMulti :: (HasCallStack, MemoizeMultiContainer f, Members '[ErrorE, Memoize] r, Typeable a) =>
  Bool -> Severity -> Maybe Text -> (b -> MemoizeKey) -> f b -> (f b -> Sem r (f (b, a)))
  -> Sem r (f (b, a))
runMemoizedMulti ignoreCache log msg f k a = withFrozenCallStack
  -- this combination of `withFrozenCallStack` and `withCallStackLoc` is important for this function to work correctly
  $ withCallStackLoc
  $ \loc -> snd <$> unsafeMemoizeMulti ignoreCache log msg loc f k () (fmap2 ((),) a)
{-# INLINABLE runMemoizedMulti #-}

-- | this will rerun the action if the cached value contains an outdated (maximum) mod time for the given paths
--
-- the path will be added to the key
runMemoizedWithModTime :: (Typeable a, Members [Embed IO, Memoize, ErrorE] r)
  => Severity -> Maybe Text -> MemoizeKey -> NonEmpty FilePath -> Sem r a -> Sem r a
runMemoizedWithModTime sev msg key paths action = withFrozenCallStack $ do
  currentModTime <- getModTime
  (cachedModTime, cachedContent) <- readMemoized False
  bool (snd <$> readMemoized True) (pure cachedContent) $ currentModTime <= cachedModTime
  where readMemoized recalc = runMemoized2 recalc sev msg (MemoizeKey (paths,key))
          $ (,) <$> getModTime <*> action
        getModTime = embedCatch $ maximum <$> mapM getModificationTime paths
  

devmainA = runMemoizedForeignStoreInIO $ runMemoizedWithModTime Info Nothing (MemoizeKey ())
  (pure "/tmp/a") $ embedCatch $ B.readFile "/tmp/a"

devmainB = runMemoizedForeignStoreInIO $ runMemoizedWithModTime Info Nothing (MemoizeKey ())
  (pure "/tmp/a") $ embedCatch $ B.readFile "/tmp/a"

-- * Helpers 

-- | this only works if called with frozen callstack
withCallStackLoc :: (HasCallStack, TE r) => (SrcLoc -> Sem r a) -> Sem r a
withCallStackLoc = forMaybe (throw "Did you add the 'HasCallStack' constraint")
  $ snd <$> headMaybe (getCallStack callStack)
{-# INLINABLE withCallStackLoc #-}

newCache :: IO (IORef (C.Cache k v))
newCache = newIORef =<< C.newCache Nothing
{-# INLINABLE newCache #-}


recoverCacheFromStore :: forall k v . (Typeable k, Typeable v) => Word32 -> IO (IORef (C.Cache k v))
recoverCacheFromStore foreignStoreIndex = FS.lookupStore foreignStoreIndex >>= \case
  Just st -> ffor (FS.readStore st) $ \d -> fromDyn d $ P.error $ "type mismatch, expecting "
    <> show (typeRep $ Proxy @(C.Cache k v)) <> " got " <> show (dynTypeRep d)
  Nothing -> newCache >>= \r -> slap r (FS.writeStore (FS.Store foreignStoreIndex) $ toDyn r)
{-# INLINABLE recoverCacheFromStore #-}

clearMemoizeCache :: IO ()
clearMemoizeCache = FS.deleteStore @() $ FS.Store memoizeForeignStoreIndex

recoverMemoizeCacheFromStore :: IO (IORef MemoizeCache)
recoverMemoizeCacheFromStore = recoverCacheFromStore memoizeForeignStoreIndex
