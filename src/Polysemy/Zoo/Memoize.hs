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

import qualified Data.Cache as C
import qualified Data.HashSet as HS
import           Data.Serialize.Text ()
import qualified Data.Time as B
import           Data.Vector.Serialize ()
import           Foreign.Store
import           GHC.Stack (SrcLoc(..))
import           Polysemy.AtomicState
import qualified Polysemy.Cache as PC
import           Polysemy.Log
import           Polysemy.Zoo.Prelude hiding (SrcLoc)
import           Polysemy.Zoo.Utils
import qualified Prelude as P
import qualified Type.Reflection as R

type CacheKey                   = (TypeRep, MemoizeKey, MinimalSrcLoc)
type MemoizeCache               = C.Cache CacheKey Any
type MemoizeCacheEffect         = PC.Cache CacheKey Any
type MemoizeCacheStateEffect    = AtomicState MemoizeCache
type MinimalSrcLoc              = (FilePath, Int, Int)
type MemoizeKey                 = [Key]

data Key = forall a . (Eq a, Typeable a, Hashable a, Show a) => Key a

instance Show Key where
  show (Key a) = show a

instance Eq Key where
  (Key a) == (Key b) | Just R.HRefl <- R.typeOf a `R.eqTypeRep` R.typeOf b = a == b
                     | True = False

instance Hashable Key where
  hashWithSalt salt = withKey $ hashWithSalt salt

withKey :: (forall a . (Hashable a, Show a) => a -> b) -> Key -> b
withKey f (Key a) = f a

type MemoizeMultiContainer f = (Container f, Traversable f)

data Memoize m a where
  UnsafeMemoize         :: Typeable a => Severity -> SrcLoc -> MemoizeKey -> m a -> Memoize m a
  UnsafeMemoizeMulti    :: (MemoizeMultiContainer f, Typeable a) => Severity -> SrcLoc
                        -> (b -> MemoizeKey) -> f b -> (f b -> m (f (b, Maybe a))) -> Memoize m (f (b,Maybe a))

makeSem ''Memoize

-- * Interpreters

runMemoizeInCacheEffect :: forall a r . (HasCallStack, Members '[MemoizeCacheEffect, Log, GhcTime] r)
  => Sem (Memoize : r) a -> Sem r a
runMemoizeInCacheEffect = interpretH toTactical
{-# INLINABLE runMemoizeInCacheEffect #-}

toTactical :: forall m r b (rInitial :: EffectRow) . Members '[MemoizeCacheEffect, Log, GhcTime] r
  => Memoize (Sem rInitial) b -> Tactical m (Sem rInitial) r b
toTactical = \case
  UnsafeMemoizeMulti sev loc keyF inputs action -> multi sev loc keyF inputs action
  UnsafeMemoize sev loc key action -> PC.lookup fullKey >>= maybe miss hit
    where fullKey@(tr, _, _) = getFullKey (Proxy @b) loc key
          showKey = mconcat [": ", shot key, ", Type ", shot tr]
          hit entry = (log (lowerSev sev) $ "Memoize cache hit" <> showKey) >> pureT (unsafeCoerce entry)
          miss = measureLog sev (const $ "Memoize cache miss" <> showKey) $ do
            r <- runTSimpleForce action
            pureT r <* seq r (PC.insert fullKey $ unsafeCoerce r)
{-# INLINABLE toTactical #-}

multi :: forall f b a m r (rInitial :: EffectRow) .
  (MemoizeMultiContainer f, Typeable a, Members '[MemoizeCacheEffect, Log, GhcTime] r)
  => Severity -> SrcLoc -> (b -> MemoizeKey) -> f b ->
  (f b -> Sem rInitial (f (b, Maybe a))) -> Tactical m (Sem rInitial) r (f (b, Maybe a))
multi sev loc keyF inputs action = do
  (recalc, (hits, hitKeys)) <- fmap (second containerUnzip . containerPartitionEithers)
    $ forM inputs $ \i ->
    let k = keyF i
        fk = getFk k in maybe (Left (i, k)) (Right . (,k) . (i,) . unsafeCoerce) <$> PC.lookup fk
  log (lowerSev sev) $ msg "hits" <> shlist hitKeys
  
  if null recalc then pureT hits else do
    res <- fmap (extract2 $ keyF . fst) $ measureLog sev
      (const $ msg "misses" <> shot (length recalc) <> shlist (snd <$> recalc))
      $ runTSimpleForce $ action $ fst <$> recalc 
    let done = HS.fromList (toList $ snd <$> res)
        missing = (\(i,k) -> ((i,Nothing),k)) <$> containerFilter (not . flip HS.member done . snd) recalc
        resWithMissing = containerAppend res missing
    pureT . containerAppend hits =<< forM resWithMissing (\((i,r), k) -> (i,r) <$ seq r (PC.insert (getFk k) $ unsafeCoerce r))

  where getFk = getFullKey (Proxy @a) loc
        msg x = "Memoize multi cache for type " <> shot (typeRep $ Proxy @a) <> ", " <> x <> ": "
        shlist y = case toList y of {[] -> "<None>"; y -> "\n" <> unlinesT (shot <$> y) }
{-# INLINABLE multi #-}

getFullKey :: Typeable a => Proxy a -> SrcLoc -> MemoizeKey -> CacheKey
getFullKey p loc key = (typeRep p, key, (srcLocFile loc, srcLocStartLine loc, srcLocStartCol loc))
{-# INLINABLE getFullKey #-}

runMemoizeCacheEffect :: (Member (Final IO) r, Member (Embed IO) r) =>
      Maybe (IORef MemoizeCache) -> Sem (MemoizeCacheEffect : MemoizeCacheStateEffect : r) b -> Sem r b
runMemoizeCacheEffect cacheRefM = bind (maybe (embedFinal newMemoizeCache) pure cacheRefM)
                               . flip runAtomicStateIORef . PC.runCacheAtomicState
{-# INLINABLE runMemoizeCacheEffect #-}

runMemoize :: Members '[Final IO, Embed IO, Log, GhcTime] r => Maybe (IORef MemoizeCache) -> 
  Sem (Memoize : MemoizeCacheEffect : MemoizeCacheStateEffect : r) a -> Sem r a
runMemoize cacheRefM = runMemoizeCacheEffect cacheRefM . runMemoizeInCacheEffect
{-# INLINABLE runMemoize #-}

runMemoizeInIO :: Sem '[Memoize, MemoizeCacheEffect, MemoizeCacheStateEffect, Log
                       , Error Text, GhcTime, Embed IO, Final IO] a -> IO a
runMemoizeInIO = runFinal . embedToFinal . interpretTimeGhc . errorToIOFinalThrow . interpretLogStderr . runMemoize Nothing
{-# INLINABLE runMemoizeInIO #-}

runMemoizedForeignStore :: Members '[Final IO, Embed IO, Log, GhcTime] r
                        => Sem (Memoize : MemoizeCacheEffect : MemoizeCacheStateEffect : r) a -> Sem r a
runMemoizedForeignStore ac = do ref <- embedFinal recoverMemoizeCacheFromStore
                                runMemoize (Just ref) ac
{-# INLINABLE runMemoizedForeignStore #-}

-- * Actions

runMemoized :: (TE r, HasCallStack, Typeable a, Members '[Memoize] r) => Severity -> MemoizeKey -> Sem r a -> Sem r a
runMemoized sev k a = withCallStackLoc $ \loc -> unsafeMemoize sev loc k a
{-# INLINABLE runMemoized #-}

runMemoizedMulti :: (HasCallStack, MemoizeMultiContainer f, Members '[ErrorE, Memoize] r, Typeable a) =>
  Severity -> (b -> MemoizeKey) -> f b -> (f b -> Sem r (f (b, Maybe a))) -> Sem r (f (b, Maybe a))
runMemoizedMulti log f k a = withCallStackLoc $ \loc -> unsafeMemoizeMulti log loc f k a
{-# INLINABLE runMemoizedMulti #-}

-- * Helpers 

withCallStackLoc :: (HasCallStack, TE r) => (SrcLoc -> Sem r a) -> Sem r a
withCallStackLoc = forMaybe (throw "Did you add the 'HasCallStack' constraint") $
  fmap snd $ headMaybe . snd =<< uncons (getCallStack callStack)
{-# INLINABLE withCallStackLoc #-}

newMemoizeCache :: IO (IORef (C.Cache k v))
newMemoizeCache = newIORef =<< C.newCache Nothing
{-# INLINABLE newMemoizeCache #-}

memoizeForeignStoreIndex :: Word32
memoizeForeignStoreIndex = 3333333333

recoverMemoizeCacheFromStore :: forall k v . (Typeable k, Typeable v) => IO (IORef (C.Cache k v))
recoverMemoizeCacheFromStore = lookupStore memoizeForeignStoreIndex >>= \case
  Just st -> ffor (readStore st) $ \d -> fromDyn d $ P.error $ "type mismatch, expecting "
                                         <> show (typeRep $ Proxy @(C.Cache k v)) <> " got " <> show (dynTypeRep d)
  Nothing -> newMemoizeCache >>= \r -> slap r (writeStore (Store memoizeForeignStoreIndex) $ toDyn r)
{-# INLINABLE recoverMemoizeCacheFromStore #-}

