{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}

module System.IO.Streams.SequenceId (
    sequenceIdInputStream,
    sequenceIdOutputStream,
) where

import Control.Applicative ((<$>))
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.SequenceId (
    SequenceIdError,
    checkSeqId,
    incrementSeqId,
 )
import System.IO.Streams (InputStream, OutputStream)
import qualified System.IO.Streams as Streams


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
-- ghci> resetSeqId 0
-- 3
-- ghci> 'System.IO.Streams.read' is'
-- *** Exception: user error ('Data.SequenceId.SequenceIdError' {errType = 'Data.SequenceId.SequenceIdDropped', lastSeqId = 0, currSeqId = 4})
-- @
sequenceIdInputStream ::
    Integral s =>
    -- | Initial sequence ID
    s ->
    -- | Function applied to each element of the stream to get the sequence ID
    (a -> s) ->
    -- | Error handler
    (SequenceIdError s -> IO ()) ->
    -- | 'System.IO.Streams.InputStream' to check the sequence of
    InputStream a ->
    -- | Pass-through of the given stream, and 'IO' action that returns
    -- the current sequence id and then resets it to the initial seed
    IO (InputStream a, s -> IO s)
sequenceIdInputStream initSeqId getSeqId seqIdFaultHandler =
    inputFoldM f initSeqId
  where
    f lastSeqId x = do
        let currSeqId = getSeqId x
        traverse_ seqIdFaultHandler $ checkSeqId lastSeqId currSeqId
        return $ max currSeqId lastSeqId


-- very slightly modified version of Streams.inputFoldM
inputFoldM ::
    -- | fold function
    (a -> b -> IO a) ->
    -- | initial seed
    a ->
    -- | input stream
    InputStream b ->
    -- | returns a new stream as well as an IO action to fetch and reset
    -- the updated seed value.
    IO (InputStream b, a -> IO a)
inputFoldM f initial stream = do
    ref <- newIORef initial
    is <- Streams.makeInputStream (rd ref)
    return (is, fetchAndReset ref)
  where
    twiddle _ Nothing = return Nothing
    twiddle ref mb@(Just x) = do
        !z <- readIORef ref
        !z' <- f z x
        writeIORef ref z'
        return mb

    rd ref = Streams.read stream >>= twiddle ref

    fetchAndReset ref newSeed = atomicModifyIORef' ref (newSeed,)


------------------------------------------------------------------------------

-- | Wrap an 'System.IO.Streams.OutputStream' to give a sequence ID for each element written.
--
-- Example:
--
-- @
-- ghci> (os, getList) <- 'System.IO.Streams.listOutputStream' :: 'IO' ('System.IO.Streams.OutputStream' ('Int','Int'), 'IO' [('Int','Int')])
-- ghci> (outStream', resetSeqId) <- 'sequenceIdOutputStream' 0 (\seqId a -> (seqId, a)) os
-- ghci> 'System.IO.Streams.write' (Just 6) outStream'
-- ghci> 'System.IO.Streams.write' (Just 7) outStream'
-- ghci> getList
-- [(1,6),(2,7)]
-- ghci> resetSeqId 0
-- 2
-- ghci> 'System.IO.Streams.write' (Just 6) outStream'
-- ghci> 'System.IO.Streams.write' (Just 7) outStream'
-- ghci> getList
-- [(1,6),(2,7)]
-- @
sequenceIdOutputStream ::
    Integral s =>
    -- | Initial sequence ID
    s ->
    -- | Transformation function
    (s -> a -> b) ->
    -- | 'System.IO.Streams.OutputStream' to count the elements of
    OutputStream b ->
    -- | returns a new stream as well as an 'IO' action that
    -- returns the current sequence id and then resets it to
    -- the initial seed
    IO (OutputStream a, s -> IO s)
sequenceIdOutputStream i f = outputFoldM f' i
  where
    f' seqId bdy = (nextSeqId, f nextSeqId bdy)
      where
        nextSeqId = incrementSeqId seqId


-- very slightly modified version of Streams.outputFoldM
outputFoldM ::
    Integral a =>
    -- | fold function
    (a -> b -> (a, c)) ->
    -- | initial seed
    a ->
    -- | output stream
    OutputStream c ->
    -- | returns a new stream as well as an IO action to fetch and
    -- reset the updated seed value.
    IO (OutputStream b, a -> IO a)
outputFoldM step initSeqId outStream = do
    ref <- newIORef initSeqId
    (,fetchAndReset ref) <$> Streams.makeOutputStream (wr ref)
  where
    wr _ Nothing = Streams.write Nothing outStream
    wr ref (Just x) = do
        !accum <- readIORef ref
        let (!accum', !x') = step accum x
        writeIORef ref accum'
        Streams.write (Just x') outStream

    fetchAndReset ref newSeed = atomicModifyIORef' ref (newSeed,)
