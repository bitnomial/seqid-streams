import Test.Tasty
import Test.Tasty.HUnit

import Control.Exception (Exception, throw, try)
import Data.List
import Data.Ord
import Data.SequenceId (SequenceIdError (..), SequenceIdErrorType (..))

import qualified System.IO.Streams as Streams
import System.IO.Streams.SequenceId (sequenceIdInputStream, sequenceIdOutputStream)


main = defaultMain tests


newtype SeqIdException = SeqIdException (SequenceIdError Int) deriving (Show, Eq)
instance Exception SeqIdException


tests :: TestTree
tests =
    testGroup
        "seqid-streams tests"
        [ testCase "sequenceIdInputStream can be reset" $ do
            is <- Streams.fromList [1, 2, 3, 1, 5, 2 :: Int]
            (is', resetSeqId) <- sequenceIdInputStream 0 id (throw . SeqIdException) is
            r1 <- Streams.read is'
            r1 @?= Just 1

            r2 <- Streams.read is'
            r2 @?= Just 2

            r3 <- Streams.read is'
            r3 @?= Just 3

            s1 <- resetSeqId 0
            s1 @?= 3

            r4 <- Streams.read is'
            r4 @?= Just 1

            s2 <- resetSeqId 4
            s2 @?= 1

            r5 <- Streams.read is'
            r5 @?= Just 5

            res <- try (Streams.read is')
            case res of
                Left e -> e @?= SeqIdException (SequenceIdError SequenceIdDuplicated 5 2)
                Right _ -> assertFailure "Streams.read should have failed!"

            pure ()
        , testCase "sequenceIdOutputStream can be reset" $ do
            (os, getList) <- Streams.listOutputStream :: IO (Streams.OutputStream (Int, Int), IO [(Int, Int)])
            (outStream', resetSeqId) <- sequenceIdOutputStream 0 (,) os
            Streams.write (Just 6) outStream'
            Streams.write (Just 7) outStream'

            l1 <- getList
            l1 @?= [(1, 6), (2, 7)]

            s1 <- resetSeqId 0
            s1 @?= 2

            Streams.write (Just 8) outStream'
            Streams.write (Just 9) outStream'

            l2 <- getList
            l2 @?= [(1, 8), (2, 9)]

            s1 <- resetSeqId 10
            s1 @?= 2

            Streams.write (Just 10) outStream'
            Streams.write (Just 11) outStream'

            l2 <- getList
            l2 @?= [(11, 10), (12, 11)]

            pure ()
        ]
