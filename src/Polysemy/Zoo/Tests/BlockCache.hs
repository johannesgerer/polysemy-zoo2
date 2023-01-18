{-# LANGUAGE ViewPatterns #-}
module Polysemy.Zoo.Tests.BlockCache where

import           Chronos hiding (now, second)
import qualified Data.Map as M
import qualified Data.Set as S
import           Database.SQLite.Simple
import           Polysemy.Log
import           Polysemy.State
import qualified Polysemy.State as S
import           Polysemy.Time hiding (second, hour, minute)
import           Polysemy.Zoo.BlockCache
import           Polysemy.Zoo.Prelude hiding (now, minute)
import           Polysemy.Zoo.Python
import           Polysemy.Zoo.Utils
import qualified Prelude as P
import           System.IO.Temp
import           Test.Hspec
import           Torsor

devmain = hspec . describe "BlockCache (Clear=True)"      =<<
   blockCacheTests (blockApi, False) Warn True True "/tmp"

allBlockCacheTests :: HasCallStack => IO [Spec]
allBlockCacheTests = sequence
  [describe "BlockCache (Clear=True)"      <$>
   withSystemTempDirectory "polysemy_zoo_tests" (asd True)
  ,describe "BlockCache"                   <$>
   withSystemTempDirectory "polysemy_zoo_tests" (asd False)
  ,describe "BlockCache (Keep=True)"       <$>
   withSystemTempDirectory "polysemy_zoo_tests" (asd False)
  ]
  where asd = blockCacheTests (blockApi, False) Warn True

 -- s1 <- describe "BlockCache (K" <$> withSystemTempDirectory "polysemy_zoo_tests" (blockCacheTests False)

type D1Cols     = (EntityKey, Day,           Double, Text)
type D1FfCols   = (EntityKey, Day,      Day, Double, Text)
type D2FfCols   = (EntityKey, Day,      Day, Day, Double, Text)
type D1ResCols  = D1Cols

adj :: TimeUnit u => Members '[Log, Embed IO, GhcTime] m => u -> Sem m ()
adj x = adjust x >> (log Info . ("adjusted to: " <>) . shot =<< now)


-- blockCacheTests :: Severity -> Bool -> Bool -> FilePath -> IO (Spec)


blockCacheTests :: (HasCallStack, SourceConn conn) =>
  (BlockSource conn (Sem [Python, FilesystemCache, Reader PythonConfig, BlockCache, Log, ErrorE,
             GhcTime, State (Day, Map EntityKey Int), Embed IO, Final IO]), Bool)
  -> Severity -> Bool -> Bool -> FilePath -> IO Spec
blockCacheTests (selectedApi,usePythonApi) sev keep clear dir = do
  runBlockCacheInIOConstant (Just sev) "2020-02-05 10:00" ("1990-01-01" :: Day, mempty :: Map EntityKey Int) $
   runPythonInEmbedIONoCache (PythonConfig "python" Nothing id) $ do
    openCacheConnection sev clear dir d1 (putStrLn <$ guard False) -- trace

    (r,_) <- sc False

    let sp1 = it "should correctly return initial data" $ fromList r `shouldBe` res1


    adj $ Minutes 6
    state <- getCounts
    (r2,_) <- sc False -- trace
    print . ("ASD1"::Text,) =<< scTable False
    state2 <- getCounts

    let sp2 = do np $ it "should not request more data" $ state2 `shouldBe` state
                 it "should return the same initial data" $ fromList r2 `shouldBe` res1
    
    r2' <- req5
    state2' <- getCounts

    let sp2' = do np $ it "should not request anything" $ state2' `shouldBe` state
                  it "should get the (out-of-date) intraday value"
                    $ r2' `shouldBe` [("ent1","2020-02-05",1058884.0,"ent1: 2020-02-05Intraday")]
    
    adj $ Hours 7
    r3 <- fst <$> sc False
    state3 <- getCounts

    let sp3 = do np $ it "should only request ent2" $ state3 `shouldBe` addCounts state (M.singleton "ent2" 1)
                 it "should have updated ent2 to the close" $ fromList r3 `shouldBe` resx "Close"
    
    adj $ Days 2
    r4 <- scx2 False $ reqx2 $ IntervalDateRequest dontAgg $ Right [("2020-02-06","2020-02-07","ent1")
                                                                   ,("2020-02-05","2020-02-05","ent2")]
    (startDate, state4) <- S.get

    let sp4 = do np $ it "should request both" $ state4 `shouldBe` addCounts state3 (fromList [("ent1", 1), ("ent2", 1)])
                 it "results should be correct" $ fromList r4 `shouldBe` intvres
                 np $ it "should update from a certain date" $ startDate `shouldBe` "2020-02-04"
        intvres = S.fromList [("ent1","2020-02-06","2020-02-06","2020-02-07",1058885.0,"ent1: 2020-02-06CloseH")
                             ,("ent1","2020-02-07","2020-02-06","2020-02-07",1058886.0,"ent1: 2020-02-07Close")
                             ,("ent2","2020-02-05","2020-02-05","2020-02-05",2058884.0,"ent2: 2020-02-05CloseH")]

    adj $ Minutes 6
    r5 <- req5
    state5 <- getCounts

    let sp5 = do it "should not request anything" $ state5 `shouldBe` state4
                 it "should get the updated close value"
                    $ r5 `shouldBe` [("ent1","2020-02-05",1058884.0,"ent1: 2020-02-05CloseH")]


    adj $ Minutes 1
    r6 <- req6
    state6 <- getCounts

    let sp6 = do it "should not request anything" $ state6 `shouldBe` state5
                 it "should get the value as of cut off date"
                    $ r6 `shouldBe` [("ent1","2020-02-05","2020-02-03",1058882.0,"ent1: 2020-02-03CloseH")]

    -- let sp5 = pass
    -- let sp4 = pass
    -- let sp3 = pass
    -- let sp2 = pass
    -- let sp2' = pass

    -- mapM print r3
    pure $ sequence_ [sp1, sp2, sp2', sp3, sp4, sp5, sp6]
    where reqx includeDataDate l tr = BlockCacheRequest Info dir (putStrLn <$ guard tr)
            -- (scale ((-24*4 + 10)*60 + 7) minute <$ guard True) -- error
            (scale ((-24*4 + 10)*60 + 6) minute <$ guard True) -- no error
            (fromList [scale 17 hour, scale 23 hour])
            (KeepTempTables keep) (IncludeOtherDateColumns includeDataDate) (UpdateAllEntities False) l
          reqx2 l tr = BlockCacheRequest Info dir (putStrLn <$ guard tr) (scale ((-24*4 + 10)*60 + 6) minute <$ guard True)
                      (fromList [scale 17 hour, scale 35 hour])
                      (KeepTempTables keep) (IncludeOtherDateColumns True) (UpdateAllEntities False) l
          req = reqx True $ ExactDateRequest (ForwardFillDays (False, Just 3))
            [("2020-02-04","ent1")
            ,("2020-02-03","ent2")
            ,("2020-02-07","ent2")
            ,("2020-02-07","ent3")
            ,("2020-02-09","ent2")]
          req5 = scx False $ reqx False $ ExactDateRequest LeaveMissing [("2020-02-05","ent1")]

          req6 = blockCachedConsume d1 (reqx2 req False) selectedApi $ consumer @D1FfCols
            where req = ExactDateRequest (CutoffDate "2020-02-03") [("2020-02-05","ent1")]
    
          -- consumer :: ([a], [a] -> (Only (Maybe Text) :. a) -> IO [a])
          -- consumer = ([], \acc (_ :. r) -> pure $ r:acc)

          consumer :: ([a], [a] -> a -> IO [a])
          consumer = ([], \acc r -> pure $ r:acc)
          
          
          d1 = BlockCacheDescr "t1" (("c1", "real") :|[("c2","text")]) "2020-02-02"
  
          sc tr = blockCachedConsumeWithErrors d1 (req tr) selectedApi $ consumer @D1FfCols
          scTable tr = blockCachedTable d1 (req tr) selectedApi
          scx tr r = blockCachedConsume d1 (r tr) selectedApi $ consumer @D1ResCols
          scx2 tr r = blockCachedConsume d1 (r tr) selectedApi $ consumer @D2FfCols
          resx c = S.fromList [("ent2","2020-02-07","2020-02-05",2058884.0,"ent2: 2020-02-05" <> c)
                              ,("ent2","2020-02-03","2020-02-03",2058882.0,"ent2: 2020-02-03CloseH")
                              ,("ent1","2020-02-04","2020-02-04",1058883.0,"ent1: 2020-02-04CloseH")]
          res1 = resx "Intraday"
          getCounts = snd <$> S.get

          np :: Applicative f => f () -> f ()
          np a = bool a pass usePythonApi 


testPython = hspec . describe "BlockCache" . sequence_  =<<
  forM [True,False] (\k -> blockCacheTests (blockApi, False) Warn k True "/tmp"
                           >> blockCacheTests (pythonApi, True) Warn k True "/tmp")

-- select d.*, date(bcDate + 2400000.5,'auto') as date, bcKey, datetime(scLastUpdate,'auto') as up from data d left join entities e on e.scEntityId = d.scEntityId;

pythonApi
  :: (Member GhcTime r, Member Polysemy.Zoo.Python.Python r) =>
     BlockSource FilePath (Sem r)
pythonApi = pythonSource $ (id,) $ \req -> fmap ((PurgeExisting False,) . ([],)) $ do
  (m, end) <- helper
  -- print True
  pure [qc|
def testRow(d,m,e):
        d2 = modifiedJulianToDate(d)
        res = (e, d2, d + (1e6 if e == 'ent1' else 2e6), e + ": " + str(d2) + m)
        # print(res)
        return res

def requestBlocks(blocks):

        # print('111')
        # raise Exception('asd')
        res = []
        end = {getDay end}
        for (sdate, ents) in blocks:
                rows = []
                errs = []
                print(ents)
                for e in list(ents):
                    if e == 'ent3':
                        errs.append(\{'bcKey': e, 'bcEntityError': "python test error"}) 
                        continue
                    for d in range({getDay $ bdStartDate $ srCacheDescription req}, end):
                        rows.append(testRow(d,"CloseH",e))
                    for m in {m}:
                        rows.append(testRow(end,m,e))
                df = pd.DataFrame(columns=['bcKey','bcDate','c1','c2'], data=rows)
                res.append((df, pd.DataFrame(errs)))
                
        return res
                
|]

blockApi :: Members '[Log, ErrorE, Embed IO, GhcTime, S.State (Day, Map EntityKey Int)] m
  => BlockSource Connection (Sem m)
blockApi = haskellSource $ fmap (PurgeExisting False,) . mapM rawBlockData . fmap (second toList)

rawBlockData :: (Members '[GhcTime, S.State (Day, Map EntityKey Int), Embed IO] r)
  => (Day, [EntityKey]) -> Sem r ([D1Cols],[(EntityKey, Text)])
rawBlockData (start, partition (== "ent3") -> (errs,ents)) = do
  (m, end) <- helper
  -- print (t, ents)
  S.modify $ const start *** addCounts (fromList $ (,1) <$> ents)
  let res = (testRow <$> [start..pred end] <*> ["CloseH"] <*> ents)
        <> (testRow end <$> m <*> ents)
  -- print res
  pure (res, (, "haskell test error") <$> errs)

testRow :: Day -> Text -> EntityKey -> D1Cols
testRow d m e = (e, d, fromIntegral (getDay d) + o, coerce e <> ": " <> shot d <> m)
  where o | e == "ent1" = 1e6
          | e == "ent2" = 2e6
          | True = P.error $ "bad entity: " <> show e

helper :: Member GhcTime r => Sem r ([Text], Day)
helper = do
  n <- fromBaseUtcTime <$> now
  let t = datetimeTime (timeToDatetime n) 
      m | t < "08:00"   = []
        | t < "16:00"   = ["Intraday"]
        | True          = ["Close"]
  pure (m, timeToDayTruncate n)

        
addCounts :: (Ord k, Num a) => Map k a -> Map k a -> Map k a
addCounts = M.unionWith (+)

dontAgg = DoAggregateIntervals False
