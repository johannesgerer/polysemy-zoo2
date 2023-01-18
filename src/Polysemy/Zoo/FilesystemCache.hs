{-# LANGUAGE TemplateHaskell #-}

module Polysemy.Zoo.FilesystemCache
  (module Polysemy.Zoo.FilesystemCache
  ,module Reexport) where



import           Codec.Serialise as S
import qualified Data.ByteString as B
import qualified Data.Cache as C
import qualified Data.Map as M
import           Data.Serialize as Z
import           Data.Serialize.Text ()
import qualified Data.Set as S
import           Data.Time (UTCTime)
import           Data.Vector.Serialize ()
import           Hoff.Serialise
import           Polysemy.Time hiding (second)
import           Polysemy.Zoo.Md5 as Reexport
import           Polysemy.Zoo.Memoize (recoverCacheFromStore)
import           Polysemy.Zoo.Prelude
import           Polysemy.Zoo.Utils
import           System.FilePattern.Directory


-- | all constructors unsafe because they will generate wrong call site locations when logging
data FilesystemCache m a where
  UnsafeDropFsCached  :: (HasCallStack, Foldable f) => f FsCacheBucket  -> FilesystemCache m ()

  UnsafeFsCached      :: (Typeable a, HasCallStack) => (FsCacheBucket, Md5Hash) -> (a -> m ByteString)
                      -> (ByteString -> m a) -> m a -> FilesystemCache m a

  UnsafeFsMultiCached :: (Typeable a, HasCallStack) => FsCacheBucket -> [(b,Md5Hash)] -> (a -> m ByteString)
                      -> (ByteString -> m a) -> MultiGenerator w b a m -> FilesystemCache m (w, [(b,a)])

type MultiGenerator w input res m =
  [((input, Md5Hash), res)]
        -- ^ cache hits. the MultiGenerator can decide which ones to recalculate, if any
  -> [(input, Md5Hash)]
        -- ^ cache misses. these are expected to be generated by the MultiGenerator
  -> m (w -- ^ errors
        ,([Md5Hash] -- ^ cache hits that should be invalidated
        ,[((input, res), Maybe Md5Hash)] -- ^ results. Just Hash means -> cache this result, Nothing -> do not cache
-- use case: res is not complete (with errors that are returned in `w`
-- so should be recalculated and it's hash will not be returned in this list
-- but in the first list of hashes to be invalidated 
        ))

data FsCacheConfig  = FsCacheConfig
  { fscThrowOnUnknownBucket     :: Maybe (ErrorMsg -> ErrorMsg)
  -- ^ Nothing -> do not throw an error, but silently continue without caching
  , fscWithMemoryCache          :: Bool
  -- ^ if yes, a cache file will only be read once from disk. subsequent hits will be served from
  -- memory, but only if the cache file still exists and has the same modification time
  , fscFolder                   :: FilePath
  , fscTtl                      :: Map FsCacheBucket (Maybe Seconds)
                                  -- ^ Nothing -> do not cache
                                  --   Just 0 -> cache indefinitely
                                  --   Just x -> cache for x seconds   
  }

type FsCacheKey = (FsCacheBucket, Md5Hash)

type FsCacheBucket = Text
type FsCacheBuckets = Set FsCacheBucket

makeSem ''FilesystemCache

initFsCacheKey :: Md5Hashable a => FsCacheBucket -> a -> FsCacheKey
initFsCacheKey b = (b,) . md5Hash Nothing

fsCachePath :: FsCacheKey -> FilePath
fsCachePath (bucket, hash) = toS bucket </> toS (md5HashToHex hash)

dontCache :: FsCacheKey
dontCache = ("Uncached", Md5Hash "Uncached")

cacheEnabledAndTtl :: TE r => FsCacheConfig -> FsCacheBucket -> Sem r (Bool, Maybe Seconds)
cacheEnabledAndTtl c bucket = case M.lookup bucket $ fscTtl c of
  Nothing -> forMaybe (pure (False, Nothing)) (fscThrowOnUnknownBucket c)
    $ \f -> throw $ f $ "Unknown FileSystemCache bucket encountered: " <> bucket
  Just ttl -> pure (isJust ttl, bool ttl Nothing $ ttl == Just 0)

getFsMemoryCache :: Members '[Embed IO] r => Sem r FsMemoryCache
getFsMemoryCache = embed $ readIORef =<< recoverCacheFromStore fsCacheForeignStoreIndex

runFilesystemCache :: (HasCallStack, Members '[Log, ErrorE, GhcTime, Embed IO] r)
  => FsCacheConfig -> Sem (FilesystemCache ': r) a -> Sem r a
runFilesystemCache config sem = do
  memoryCacheM <- mapM (\_ -> getFsMemoryCache) $ guard $ fscWithMemoryCache config
  interpretH (\case
    UnsafeFsMultiCached bucket hashesAndInputs enc dec action ->
      common memoryCacheM config bucket
      enc dec (second (fmap fst . snd) <$> action [] hashesAndInputs) $
      \getCached writeToCache -> do
        ins <- getInspectorT
        (recalcs, cached) <- partitionEithers
          <$> mapM (\bh -> maybe (Left bh) (Right . (bh,)) <$> (mapM (forceIns ins) =<< getCached (snd bh)))
          hashesAndInputs
        (errs, (invalidated, newRes)) <- forceIns ins =<< runTSimple (action cached recalcs)
        mapM (\((_,r),h) -> writeToCache r h) $ mapMaybe sequence newRes
        let invalidatedS = S.fromList invalidated
            reusedCached = filter (\((_,h),_) -> not $ S.member h invalidatedS) cached
        
        pureT $ (errs, fmap (first fst) reusedCached <> fmap fst newRes)
    UnsafeFsCached (bucket, hash) enc dec action -> common memoryCacheM config bucket enc dec
      action $ \getCached writeToCache -> let recalc = do r <- runTSimple action
                                                          r <$ (flip writeToCache hash =<< forceT r)
                                          in maybe recalc pure =<< getCached hash
    UnsafeDropFsCached names    -> (pureT =<<) $ forM_ names $ \k -> do
      let globPattern = toS k <> "/*"
          folder = fscFolder config
      files <- embedCatch $ getDirectoryFiles folder [globPattern]
      forM_ files $ \p -> do  log Info ("droppeding " <> toS p <> "...")
                              embedCatch (removeFile $ folder </> p)
      when (null files) $ log Info $ toS $ "No cache files found in " <> folder <> " matching '"
        <> globPattern <> "'"
             ) sem

type FsMemoryCache = C.Cache (TypeRep, FilePath) (UTCTime, Any)

common :: forall r f a e1 m b e2 .
  (Typeable a, HasCallStack, Members [ErrorE, GhcTime, Embed IO, Log] r, Functor f)
  => Maybe FsMemoryCache -> FsCacheConfig
  -> FsCacheBucket -> (a -> m ByteString) -> (ByteString -> m a)
  -> m b
  -> ((Md5Hash -> Sem (WithTactics e1 f m r) (Maybe (f a))) -> (a -> Md5Hash -> Sem (WithTactics e1 f m r) ())
  -> Sem (WithTactics e2 f m r) (f b))
  -> Sem (WithTactics e2 f m r) (f b)
common memoryCacheM config bucket enc dec noCacheAction cacheAction = do
  (enabled, ttl) <- cacheEnabledAndTtl config bucket
  bool (runTSimple noCacheAction) (cacheAction (getCached ttl) writeToCache) enabled
  where getPath h = fscFolder config </> fsCachePath (bucket, h)
        getCached ttl h = do
          ex <- embedCatch $ doesFileExist path
          modtimeM <- if ex then do
            modtime <- embedCatch $ getModificationTime path
            age <- since modtime
            let expired = Just True == fmap (age >=) ttl
            when expired $ logm "expired"
            (modtime <$ guard (not expired)) <$ log Trace (shot (ttl, age))
            else Nothing <$ logm "miss"
          forM modtimeM $ \modtime -> do
            let readFs = do
                  logm "hit (file system)"
                  val <- runTSimpleForce . dec =<< embedCatch (B.readFile path)
                  embed $ insert path modtime val
                  pureT val
                readMemory (mt,val) | modtime == mt = pureT val <* logm "hit (memory)"
                                    | True          = readFs 
            catch (maybe readFs readMemory =<< embed (lookup path))
              $ \e -> throw $ "Exception when reading or decoding FileSystemCache path "
                      <> toS path <> ":\n" <> e
          where path = getPath h
                logm msg = log Debug (msg <> " " <> toS path)
        writeToCache v h = runTSimpleForce (enc v) >>= \bs -> embedCatch $ do
          createDirectoryIfMissing True (takeDirectory path)
          B.writeFile path =<< seq (B.length bs) (pure bs)
          modtime <- getModificationTime path
          insert path modtime v
          where path = getPath h
        (insert, lookup) = insertAndLookup memoryCacheM

insertAndLookup :: forall a b c . (Hashable c, Typeable b) => Maybe (C.Cache (TypeRep, c) (a, Any))
  -> (c -> a -> b -> IO ()
     ,c -> IO (Maybe (a, b)))
insertAndLookup cacheM =
  (\key modtime v      -> forM_ cacheM
    $ \c -> C.insert c (typeRepA, key) (modtime, (unsafeCoerce :: b -> Any) v)
  ,\key                -> fmap (fmap2 (unsafeCoerce :: Any -> b) . join)
    $ forM cacheM $ \c -> C.lookup c (typeRepA, key))
  where typeRepA = typeRep $ Proxy @b
                          

-- * Actions

type FsCache r = (HasCallStack, Members '[FilesystemCache] r)

fsCachedSerialized :: (Typeable a, Serialize a, Members '[ErrorE] r, FsCache r)
  => FsCacheKey -> Sem r a -> Sem r a
fsCachedSerialized ck = withFrozenCallStack $ unsafeFsCached ck (pure . Z.encode) (fromEitherWith toS . Z.decode)
{-# INLINE fsCachedSerialized #-}

fsCachedLByteString :: FsCache r => FsCacheKey -> Sem r LByteString -> Sem r LByteString
fsCachedLByteString ck = withFrozenCallStack $ unsafeFsCached ck (pure . toS) $ pure . toS
{-# INLINE fsCachedLByteString #-}

fsCachedByteString :: FsCache r => FsCacheKey -> Sem r ByteString -> Sem r ByteString
fsCachedByteString ck = withFrozenCallStack $ unsafeFsCached ck pure pure
{-# INLINE fsCachedByteString #-}

fsCachedSerialised :: (Typeable a, Serialise a, Members '[ErrorE] r, FsCache r)
  => FsCacheKey -> Sem r a -> Sem r a
fsCachedSerialised ck = withFrozenCallStack $ unsafeFsCached ck (pure . toS . S.serialise)
  $ fromEitherWith shot . S.deserialiseOrFail . toS
{-# INLINE fsCachedSerialised #-}

fsCachedSerialisable :: (Typeable a, Serialisable a, Members '[ErrorE] r, FsCache r)
  => FsCacheKey -> Sem r a -> Sem r a
fsCachedSerialisable ck = withFrozenCallStack $ unsafeFsCached ck
  (fmap (toS . S.serialise) . fromEither . ensureEncodable)
  $ fromEitherWith shot . (fmap fromDecodeOnly . S.deserialiseOrFail) . toS
{-# INLINE fsCachedSerialisable #-}


fsMultiCachedSerialized :: (Typeable a, Serialize a, Members '[ErrorE] r, FsCache r)
  => FsCacheBucket -> [(b, Md5Hash)] -> MultiGenerator w b a (Sem r) -> Sem r (w,[(b,a)])
fsMultiCachedSerialized b a = withFrozenCallStack $ unsafeFsMultiCached b a (pure . Z.encode)
  $ fromEitherWith toS . Z.decode
{-# INLINE fsMultiCachedSerialized #-}

fsMultiCachedLByteString :: FsCache r => FsCacheBucket
  -> [(b, Md5Hash)] -> MultiGenerator w b LByteString (Sem r) -> Sem r (w,[(b, LByteString)])
fsMultiCachedLByteString b a = withFrozenCallStack $ unsafeFsMultiCached b a (pure . toS) $ pure . toS
{-# INLINE fsMultiCachedLByteString #-}

fsMultiCachedByteString :: FsCache r => FsCacheBucket
  -> [(b, Md5Hash)] -> MultiGenerator w b ByteString (Sem r) -> Sem r (w,[(b, ByteString)])
fsMultiCachedByteString b a = withFrozenCallStack $ unsafeFsMultiCached b a pure pure
{-# INLINE fsMultiCachedByteString #-}

fsMultiCachedSerialised :: (Typeable a, Serialise a, Members '[ErrorE] r, FsCache r) =>
  FsCacheBucket -> [(b, Md5Hash)] -> MultiGenerator w b a (Sem r) -> Sem r (w,[(b, a)])
fsMultiCachedSerialised b a = unsafeFsMultiCached b a (pure . toS . S.serialise)
  $ fromEitherWith shot . S.deserialiseOrFail . toS
{-# INLINE fsMultiCachedSerialised #-}

dropFsCached :: (HasCallStack, Member FilesystemCache r, Foldable f) => f FsCacheBucket -> Sem r ()
dropFsCached = withFrozenCallStack unsafeDropFsCached
{-# INLINE dropFsCached #-}

fsCached :: (Typeable a, HasCallStack, Member FilesystemCache r, HasCallStack) => (FsCacheBucket, Md5Hash)
  -> (a -> Sem r ByteString) -> (ByteString -> Sem r a) -> Sem r a -> Sem r a
fsCached = withFrozenCallStack unsafeFsCached
{-# INLINE fsCached #-}

fsMultiCached :: (Typeable a, HasCallStack, Member FilesystemCache r, HasCallStack)
  => FsCacheBucket -> [(b, Md5Hash)]
  -> (a -> Sem r ByteString) -> (ByteString -> Sem r a) -> MultiGenerator w b a (Sem r)
  -> Sem r (w, [(b, a)])
fsMultiCached = withFrozenCallStack unsafeFsMultiCached
{-# INLINE fsMultiCached #-}
