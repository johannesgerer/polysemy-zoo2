{-# LANGUAGE TemplateHaskell #-}

module Polysemy.Zoo.FilesystemCache
  (module Polysemy.Zoo.FilesystemCache
  ,module Reexport) where



import           Codec.Serialise as S
import qualified Data.ByteString as B
import           Data.Serialize as Z
import           Data.Serialize.Text ()
import qualified Data.Set as S
import           Data.Vector.Serialize ()
import           Polysemy.Time hiding (second)
import           Polysemy.Zoo.Md5 as Reexport
import           Polysemy.Zoo.Prelude
import           Polysemy.Zoo.Utils
import           System.FilePattern.Directory


data FilesystemCache m a where
  DropFsCached  :: (HasCallStack, Foldable f) => f FsCacheBucket  -> FilesystemCache m ()

  FsCached      :: HasCallStack => (FsCacheBucket, Md5Hash) -> (a -> m ByteString) -> (ByteString -> m a)
                -> m a -> FilesystemCache m a

  FsMultiCached :: HasCallStack => FsCacheBucket -> [(b,Md5Hash)] -> (a -> m ByteString)
                -> (ByteString -> m a) -> MultiGenerator w b a m -> FilesystemCache m (w, [(b,a)])

type MultiGenerator w input res m =
  [((input, Md5Hash), res)] -- ^ cache hits. this function can decide which ones to recalculate
  -> [(input, Md5Hash)] -> m (w -- ^ errors
                             ,([Md5Hash] -- ^ cache hit that should be invalidated
                              ,[((input, res), Maybe Md5Hash)] -- ^ results
                              ))

type CacheTTL = Map FsCacheBucket (Maybe Seconds)

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

cacheEnabledAndTtl :: CacheTTL -> FsCacheBuckets -> FsCacheBucket -> (Bool, Maybe Seconds)
cacheEnabledAndTtl cacheTtl cacheBuckets bucket = (elem bucket cacheBuckets || isJust ttl, join ttl)
  where ttl = cacheTtl ^? ix bucket
          
runFilesystemCache :: (HasCallStack, Members '[Log, ErrorE, GhcTime, Embed IO] r) => FilePath
  -> CacheTTL -> FsCacheBuckets -> Sem (FilesystemCache ': r) a -> Sem r a
runFilesystemCache folder cacheTtl cacheBuckets = interpretH $ \case
  FsMultiCached bucket hashesAndInputs enc dec action -> withFrozenCallStack $
    common folder cacheTtl cacheBuckets bucket enc dec (second (fmap fst . snd) <$> action [] hashesAndInputs) $
    \getCached writeToCache -> do
      ins <- getInspectorT
      (recalcs, cached) <- partitionEithers
        <$> mapM (\bh -> maybe (Left bh) (Right . (bh,)) <$> (mapM (forceIns ins) =<< getCached (snd bh))) hashesAndInputs
      (errs, (invalidated, newRes)) <- forceIns ins =<< runTSimple (action cached recalcs)
      mapM (\((_,r),h) -> writeToCache r h) $ mapMaybe sequence newRes
      let invalidatedS = S.fromList invalidated
          reusedCached = filter (\((_,h),_) -> not $ S.member h invalidatedS) cached
      
      pureT $ (errs, fmap (first fst) reusedCached <> fmap fst newRes)
  FsCached (bucket, hash) enc dec action -> withFrozenCallStack $
    common folder cacheTtl cacheBuckets bucket enc dec action $ \getCached writeToCache ->
    let recalc = do r <- runTSimple action
                    r <$ (flip writeToCache hash =<< forceT r)
    in maybe recalc pure =<< getCached hash
  DropFsCached names    -> withFrozenCallStack $ pureT =<< mapM_ g names
    where g k   = do let globPattern = toS k <> "/*"
                     files <- embedCatch $ getDirectoryFiles folder [globPattern]
                     forM_ files $ \p -> do  log Info ("droppeding " <> toS p <> "...")
                                             embedCatch (removeFile $ folder </> p)
                     when (null files) $ log Info $ toS $ "No cache files found in " <> folder <> " matching '" <> globPattern <> "'"

common :: (HasCallStack, Members [ErrorE, GhcTime, Embed IO, Log] r, Functor f) => FilePath -> CacheTTL
       -> FsCacheBuckets -> FsCacheBucket -> (a -> m ByteString) -> (ByteString -> m a) -> m b
       -> ((Md5Hash -> Sem (WithTactics e1 f m r) (Maybe (f a))) -> (a -> Md5Hash -> Sem (WithTactics e1 f m r) ())
       -> Sem (WithTactics e2 f m r) (f b))
       -> Sem (WithTactics e2 f m r) (f b)
common folder cacheTtl cacheBuckets bucket enc dec noCacheAction cacheAction = 
  bool (runTSimple noCacheAction) (cacheAction getCached writeToCache) enabled
  where (enabled, ttl) = cacheEnabledAndTtl cacheTtl cacheBuckets bucket
        getPath h = folder </> fsCachePath (bucket, h)
        getCached h = do ex <- embedCatch $ doesFileExist path
                         sequence . ((logm "hit" >> read) <$) . guard =<< if ex then do
                           age <- since =<< embedCatch (getModificationTime path)
                           let expired = Just True == fmap (age >=) ttl
                           when expired $ logm "expired"
                           not expired <$ log Trace (shot (ttl, age))
                           else False <$ logm "miss"
          where path = getPath h
                read = catch ((\x -> runTSimple $ dec x) =<< embedCatch (B.readFile path))
                  $ \e -> throw $ "Exception when reading or decoding FileSystemCache path " <> toS path <> ":\n" <> e
                logm msg = log Debug (msg <> " " <> toS path)
        writeToCache v h = runTSimpleForce (enc v) >>= \bs -> embedCatch $ do
          createDirectoryIfMissing True (takeDirectory path)
          B.writeFile path =<< seq (B.length bs) (pure bs)
          where path = getPath h
                          
                           

-- * Actions

type FsCache r = (HasCallStack, Members '[FilesystemCache] r)

fsCachedSerialized :: (Serialize a, Members '[ErrorE] r, FsCache r) => FsCacheKey -> Sem r a -> Sem r a
fsCachedSerialized ck = withFrozenCallStack $ fsCached ck (pure . Z.encode) (fromEitherWith toS . Z.decode)
{-# INLINE fsCachedSerialized #-}

fsCachedLByteString :: FsCache r => FsCacheKey -> Sem r LByteString -> Sem r LByteString
fsCachedLByteString ck = withFrozenCallStack $ fsCached ck (pure . toS) $ pure . toS
{-# INLINE fsCachedLByteString #-}

fsCachedByteString :: FsCache r => FsCacheKey -> Sem r ByteString -> Sem r ByteString
fsCachedByteString ck = withFrozenCallStack $ fsCached ck pure pure
{-# INLINE fsCachedByteString #-}

fsCachedSerialised :: (Serialise a, Members '[ErrorE] r, FsCache r) => FsCacheKey -> Sem r a -> Sem r a
fsCachedSerialised ck = withFrozenCallStack $ fsCached ck (pure . toS . S.serialise)
  $ fromEitherWith shot . S.deserialiseOrFail . toS
{-# INLINE fsCachedSerialised #-}


fsMultiCachedSerialized :: (Serialize a, Members '[ErrorE] r, FsCache r)
  => FsCacheBucket -> [(b, Md5Hash)] -> MultiGenerator w b a (Sem r) -> Sem r (w,[(b,a)])
fsMultiCachedSerialized b a = withFrozenCallStack $ fsMultiCached b a (pure . Z.encode) (fromEitherWith toS . Z.decode)
{-# INLINE fsMultiCachedSerialized #-}

fsMultiCachedLByteString :: FsCache r => FsCacheBucket
  -> [(b, Md5Hash)] -> MultiGenerator w b LByteString (Sem r) -> Sem r (w,[(b, LByteString)])
fsMultiCachedLByteString b a = withFrozenCallStack $ fsMultiCached b a (pure . toS) $ pure . toS
{-# INLINE fsMultiCachedLByteString #-}

fsMultiCachedByteString :: FsCache r => FsCacheBucket
  -> [(b, Md5Hash)] -> MultiGenerator w b ByteString (Sem r) -> Sem r (w,[(b, ByteString)])
fsMultiCachedByteString b a = withFrozenCallStack $ fsMultiCached b a pure pure
{-# INLINE fsMultiCachedByteString #-}

fsMultiCachedSerialised :: (Serialise a, Members '[ErrorE] r, FsCache r) =>
  FsCacheBucket -> [(b, Md5Hash)] -> MultiGenerator w b a (Sem r) -> Sem r (w,[(b, a)])
fsMultiCachedSerialised b a = withFrozenCallStack $ fsMultiCached b a (pure . toS . S.serialise)
  $ fromEitherWith shot . S.deserialiseOrFail . toS
{-# INLINE fsMultiCachedSerialised #-}
