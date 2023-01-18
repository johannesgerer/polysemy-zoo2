module Polysemy.Zoo.Tests where

import           Chronos hiding (now, second)
import qualified Data.Aeson as A
import           Database.SQLite.Simple
import           Database.SQLite.Simple.FromField
import           Database.SQLite.Simple.Internal
import           Database.SQLite.Simple.Ok
import           Database.SQLite.Simple.ToField
import           Polysemy.Zoo.BlockCache
import           Polysemy.Zoo.FilesystemCache
import           Polysemy.Zoo.Prelude hiding (now)
import           Polysemy.Zoo.Tests.BlockCache hiding (devmain)
import           Polysemy.Zoo.Utils
import qualified Prelude as P
import           System.IO.Temp
import           Test.Hspec

dayFieldCases :: [(Int, (Day, SQLData))]
dayFieldCases = zip [1..] $ [("2022-03-04",SQLInteger 59642)
                            ,("1920-01-31",SQLInteger 22354)
                            ,("2300-12-12",SQLInteger 161462)]

main :: HasCallStack => IO ()
main = do
  bct <- allBlockCacheTests

  fsc <- fsCacheTests Nothing

  hspec $ do
    fsc
    
    sequence bct

    describe "Polysemy.Zoo.BlockCache.transformIntervals" $ 
      let cmp i (input,res) = it ("should satisfy case " <> show i)
            $ transformIntervals (DoAggregateIntervals True) (Right input) `shouldBe` res 
      in zipWithM_ cmp [(1::Int)..]
         [([("2021-01-02", "2021-01-02", "a")
           ,("2021-01-01", "2021-01-02", "b")
           ,("2021-01-02", "2021-01-03", "a")
           ,("2021-01-01", "2021-01-02", "a")
           ],[("2021-01-01", "2021-01-03", "a")
           ,("2021-01-01", "2021-01-02", "b")
           ])]

    describe "Database.SQLite.Simple.ToField Day" $ 
      forM_ dayFieldCases $ \(i, (input,res)) -> it ("should satisfy case " <> show i) $ toField input `shouldBe` res 

    describe "Database.SQLite.Simple.FromField Day" $ 
      forM_ dayFieldCases $ \(i, (input,res)) -> it ("should satisfy case " <> show i) $ Ok input `shouldBe` fromField (Field res 1) 
  
    describe "Polysemy.Zoo.BlockCache.cacheFile" $ it "should not change" $
      cacheFile "asd" (BlockCacheDescr "name" [("c1","type"),("c2","type2")] "20200203") `shouldBe`
      "asd/name_ef34a4c35c9003ea7046fb068a4334f4.sqlite"
 
    describe "Polysemy.Zoo.BlockCache.BlockSourceRequest JSON instance (needed in `pythonSource`)" $
      it "should not change" $ let req = BlockSourceRequest Info ("a.sqlite" :: FilePath) (Just "2022-02-24") "asd"
                                     (BlockCacheDescr "t1" [("c1", "real"), ("c2","text")] "2020-02-02")
                                     [(Day 58881, "query")]
      in A.encode req `shouldBe` "{\"srBlocks\":[[58881,\"query\"]],\"srCacheDescription\":{\"bdCols\":[[\"c1\",\"real\"],[\"c2\",\"text\"]],\"bdName\":\"t1\",\"bdStartDate\":58881},\"srConn\":\"a.sqlite\",\"srCutoffDate\":59634,\"srNewDataTable\":\"asd\",\"srSeverity\":\"Info\"}" 

fsCacheTests :: HasCallStack => Maybe Severity -> IO (SpecWith ())
fsCacheTests l =
  fmap (describe "fsMultiCachedSerialised" . sequence_) $ withSystemTempDirectory "polysemy_zoo_tests" $ \dir -> do
    runFinal . embedToFinal . interpretTimeGhc . errorToIOFinalThrow . (interpretLogStderrLevel l)
    $ runFilesystemCache dir mempty (fromList ["t1"]) $ do
      let ru args = fsMultiCachedSerialised "t1" ((\x -> (x, md5init $ showt x)) <$> args) $
            \c xs -> let recalc = P.take 1 c
                     in (bool ["a"::Text] [] (null c), (snd . fst <$> recalc
                        ,fmap (dup *** Just) $ xs <> fmap (\((_,h),v) -> (v,h)) recalc))
                        <$ mapM (print . fst) xs
      sequence [it "" . shouldBe ([],dup <$> [1,2]) <$> ru [1,2 :: Int]
               ,it "" . shouldBe (["a"],dup <$> [2,3,1]) <$> ru [1,3,2 :: Int]]

devmain = hspec =<< fsCacheTests (Just Debug)

devmain2 = fsCacheTests2 (Just Debug)
    
  

fsCacheTests2 l = withSystemTempDirectory "polysemy_zoo_tests" $ \dir -> do
    runFinal . embedToFinal . interpretTimeGhc . runError . (interpretLogStderrLevel l)
      . id -- throwWriterMsgs T.unlines
    $ runFilesystemCache dir mempty (fromList ["t1"]) $ runError . logWriterMsgs Debug $ do
      let ru args = fsMultiCachedSerialised "t1" ((\x -> (x, md5init $ showt x)) <$> args) $
            \c xs -> do
              throw ("e1" :: Text)
              putStrLn @Text "misses"
              mapM print xs
              putStrLn @Text "hits"
              mapM print c
              tell ["asd"]
              pure $ ([True], ([], fmap (dup *** Just) xs <> P.take 1 ((\((_,h),v) -> ((v,v),Just h)) <$> c)))
      print =<< ru [1,2 :: Int]
      print =<< ru [1,3,2 :: Int]


t2
  :: Sem
       '[Writer String, Writer ErrorMsgs, Log, GhcTime, Embed IO,
         Final IO]
       a
     -> IO (String, a)
t2 = runFinal . embedToFinal . interpretTimeGhc . (interpretLogStderrLevel $ Just Debug) . logWriterMsgs Debug
  . runWriterAssocR @String


-- devmain = t2 $ tell "a" >> tell ["asd"::Text]
