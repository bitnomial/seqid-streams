{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}

module System.IO.Streams.SequenceId
       ( sequenceIdInputStream
       , sequenceIdOutputStream
       ) where

import           Control.Applicative ((<$>))
import           Data.IORef          (newIORef, readIORef, writeIORef, atomicModifyIORef')
import           Data.SequenceId     (SequenceIdError, checkSeqId,
                                      incrementSeqId)
import           System.IO.Streams   (InputStream, OutputStream)
import qualified System.IO.Streams   as Streams


------------------------------------------------------------------------------
-- | Wrap an 'System.IO.Streams.InputStream' and check for dropped or duplicated sequence IDs.
--
-- Example:
--
-- @
-- ghci> is <- 'System.IO.Streams.fromList' [1..10::Int]
-- ghci> (is', resetSeqId) <- 'sequenceIdInputStream' 0 id (fail . show) is
-- ghci> 'System.IO.Streams.read' is'
-- Just 1
-- ghci> 'System.IO.Streams.read' is'
-- Just 2
-- ghci> 'System.IO.Streams.read' is'
-- Just 3
-- ghci> resetSeqId
-- 3
-- ghci> 'System.IO.Streams.read' is'
-- *** Exception: user error ('Data.SequenceId.SequenceIdError' {errType = 'Data.SequenceId.SequenceIdDropped', lastSeqId = 0, currSeqId = 4})
-- @
sequenceIdInputStream :: Integral s
                      => s                            -- ^ Initial sequence ID
                      -> (a -> s)                     -- ^ Function applied to each element of the stream to get the sequence ID
                      -> (SequenceIdError s -> IO ()) -- ^ Error handler
                      -> InputStream a                -- ^ 'System.IO.Streams.InputStream' to check the sequence of
                      -> IO (InputStream a, IO s)     -- ^ Pass-through of the given stream, and 'IO' action that returns the current sequence id and then resets it to the initial seed
sequenceIdInputStream initSeqId getSeqId seqIdFaultHandler =
    Streams.inputFoldM f initSeqId
  where
    f lastSeqId x = do
        let currSeqId = getSeqId x
        maybe (return ()) seqIdFaultHandler $ checkSeqId lastSeqId currSeqId
        return $ max currSeqId lastSeqId


------------------------------------------------------------------------------
-- | Wrap an 'System.IO.Streams.OutputStream' to give a sequence ID for each element written.
--
-- Example:
--
-- @
-- ghci> (os, getList) <- 'System.IO.Streams.listOutputStream' :: 'IO' ('System.IO.Streams.OutputStream' ('Int','Int'), 'IO' [('Int','Int')])
-- ghci> (outStream', getSeqId) <- 'sequenceIdOutputStream' 0 (\seqId a -> (seqId, a)) os
-- ghci> 'System.IO.Streams.write' (Just 6) outStream'
-- ghci> 'System.IO.Streams.write' (Just 7) outStream'
-- ghci> getList
-- [(1,6),(2,7)]
-- ghci> getSeqId
-- 2
-- ghci> 'System.IO.Streams.write' (Just 6) outStream'
-- ghci> 'System.IO.Streams.write' (Just 7) outStream'
-- ghci> getList
-- [(1,6),(2,7)]
-- @
sequenceIdOutputStream :: Integral s
                       => s                         -- ^ Initial sequence ID
                       -> (s -> a -> b)             -- ^ Transformation function
                       -> OutputStream b            -- ^ 'System.IO.Streams.OutputStream' to count the elements of
                       -> IO (OutputStream a, IO s) -- ^ returns a new stream as well as an 'IO' action that returns the current sequence id and then resets it to the initial seed
sequenceIdOutputStream i f = outputFoldM f' i
  where f' seqId bdy = (nextSeqId, f nextSeqId bdy)
          where nextSeqId = incrementSeqId seqId


outputFoldM :: Integral a
            => (a -> b -> (a, c))        -- ^ fold function
            -> a                         -- ^ initial seed
            -> OutputStream c            -- ^ output stream
            -> IO (OutputStream b, IO a) -- ^ returns a new stream as well as an IO action to fetch and reset the updated seed value.
outputFoldM step initSeqId outStream = do
    ref <- newIORef initSeqId
    let reset = atomicModifyIORef' ref (initSeqId,)
    (,reset) <$> Streams.makeOutputStream (wr ref)
  where
    wr _ Nothing    = Streams.write Nothing outStream
    wr ref (Just x) = do
        !accum <- readIORef ref
        let (!accum', !x') = step accum x
        writeIORef ref accum'
        Streams.write (Just x') outStream
