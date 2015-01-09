module System.IO.Streams.SequenceId where

import           Control.Applicative ((<$>))
import           Data.SequenceId     (SequenceError, SequenceId, checkSeqId,
                                      nextSeqId)
import           System.IO.Streams   (InputStream, OutputStream)
import qualified System.IO.Streams   as Streams


------------------------------------------------------------------------------
-- | Wrap an 'System.IO.Streams.InputStream' and check for dropped or duplicated sequence IDs.
--
-- Example:
--
-- @
-- ghci> 'System.IO.Streams.fromList' [1..10 :: 'Data.Word32'] >>= 'sequenceIdInputStream' 0 'id' ('fail' . 'show') >>= 'System.IO.Streams.toList'
-- [1,2,3,4,5,6,7,8,9,10]
--
-- ghci> 'System.IO.Streams.fromList' [5..10 :: 'Data.Word32'] >>= 'sequenceIdInputStream' 0 'id' ('fail' . 'show') >>= 'System.IO.Streams.toList'
-- *** Exception: user error (SequenceIdDropped (SequenceIds {lastSeqId = 0, currSeqId = 5}))
--
-- ghci> 'System.IO.Streams.fromList' [5..10 :: 'Data.Word32'] >>= 'sequenceIdInputStream' 1 'id' ('fail' . 'show') >>= 'System.IO.Streams.toList'
-- *** Exception: user error (SequenceIdDuplicated (SequenceIds {lastSeqId = 1, currSeqId = 1}))
-- @
sequenceIdInputStream :: SequenceId                   -- ^ Initial sequence ID
                      -> (a -> SequenceId)            -- ^ Function applied to each element of the stream to get the sequence ID
                      -> (SequenceError -> IO ())     -- ^ Error handler
                      -> InputStream a                -- ^ 'System.IO.Streams.InputStream' to check the sequence of
                      -> IO (InputStream a)           -- ^ Pass-through of the given stream
sequenceIdInputStream initSeqId getSeqId seqFaultHandler inStream = fst <$> Streams.inputFoldM f initSeqId inStream
  where
    f lastSeqId x = do
        let currSeqId = getSeqId x
            check = checkSeqId lastSeqId currSeqId
        maybe (return ()) seqFaultHandler check
        return currSeqId


------------------------------------------------------------------------------
-- | Wrap an 'System.IO.Streams.OutputStream' to give a sequence ID for each element written.
--
-- Example:
--
-- @
-- (outStream', getSeqId) <- 'sequenceIdOutputStream' 1 outStream
-- return $ 'System.IO.Streams.Combinators.contramapM' (addSeqId getSeqId) outStream'
-- @
sequenceIdOutputStream :: SequenceId                         -- ^ Initial sequence ID
                       -> OutputStream a                     -- ^ 'System.IO.Streams.OutputStream' to count the elements of
                       -> IO (OutputStream a, IO SequenceId) -- ^ ('IO' 'SequenceId') is the action to run to get the current sequence ID
sequenceIdOutputStream = Streams.outputFoldM count
  where count a _ = return $ nextSeqId a
