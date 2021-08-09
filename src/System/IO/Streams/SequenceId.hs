{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}

module System.IO.Streams.SequenceId
       ( sequenceIdInputStream
       , sequenceIdInputStreamWithReset
       , sequenceIdOutputStream
       , sequenceIdOutputStreamWithReset
       ) where

import           Control.Applicative ((<$>))
import           Control.Concurrent.MVar (newMVar, takeMVar, newEmptyMVar, putMVar, MVar, modifyMVarMasked_, modifyMVarMasked)
import           Data.IORef          (newIORef, readIORef, writeIORef, atomicModifyIORef, IORef, atomicModifyIORef')
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
-- ghci> 'System.IO.Streams.fromList' [1..10 :: 'Data.SequenceId.SequenceId'] >>= 'sequenceIdInputStream' 0 'id' ('fail' . 'show') >>= 'System.IO.Streams.toList'
-- [1,2,3,4,5,6,7,8,9,10]
--
-- ghci> 'System.IO.Streams.fromList' [5..10 :: 'Data.SequenceId.SequenceId'] >>= 'sequenceIdInputStream' 0 'id' ('fail' . 'show') >>= 'System.IO.Streams.toList'
-- *** Exception: user error ('Data.SequenceId.SequenceIdError' {errType = 'Data.SequenceId.SequenceIdDropped', lastSeqId = 0, currSeqId = 5})
--
-- ghci> 'System.IO.Streams.fromList' [1..10 :: 'Data.SequenceId.SequenceId'] >>= 'sequenceIdInputStream' 5 'id' ('fail' . 'show') >>= 'System.IO.Streams.toList'
-- *** Exception: user error ('Data.SequenceId.SequenceIdError' {errType = 'Data.SequenceId.SequenceIdDuplicated', lastSeqId = 5, currSeqId = 1})
-- @
sequenceIdInputStream :: Integral s
                      => s                            -- ^ Initial sequence ID
                      -> (a -> s)                     -- ^ Function applied to each element of the stream to get the sequence ID
                      -> (SequenceIdError s -> IO ()) -- ^ Error handler
                      -> InputStream a                -- ^ 'System.IO.Streams.InputStream' to check the sequence of
                      -> IO (InputStream a)           -- ^ Pass-through of the given stream
sequenceIdInputStream initSeqId getSeqId seqIdFaultHandler inStream =
    fst <$> Streams.inputFoldM f initSeqId inStream
  where
    f lastSeqId x = do
        let currSeqId = getSeqId x
        maybe (return ()) seqIdFaultHandler $ checkSeqId lastSeqId currSeqId
        return $ max currSeqId lastSeqId


------------------------------------------------------------------------------
-- | Same behavior as 'sequenceIdInputStream' except provides a function to reset the sequence id.
sequenceIdInputStreamWithReset :: forall s a.
                                  Integral s
                               => s                                                       -- ^ Initial sequence ID
                               -> (a -> s)                                                -- ^ Function applied to each element of the stream to get the sequence ID
                               -> (SequenceIdError s -> IO ())                            -- ^ Error handler
                               -> InputStream a                                           -- ^ 'System.IO.Streams.InputStream' to check the sequence of
                               -> IO (InputStream a, s -> IO (Maybe (SequenceIdError s))) -- ^ Pass-through of the given stream, as well as function that can reset expected seqId
sequenceIdInputStreamWithReset initSeqId getSeqId seqIdFaultHandler inStream = do
    init <- newMVar $ Right initSeqId :: IO (MVar (Either (SequenceIdError s) s))
    (,reset init) <$> Streams.mapM_ (f init) inStream
  where
    f :: MVar (Either (SequenceIdError s) s) -> a -> IO a
    f seqIdMVar x = do
        let currSeqId = getSeqId x
        modifyMVarMasked seqIdMVar $ update currSeqId
        pure x

    update currSeqId lastSeqId = do
        case lastSeqId of
          Right seqId -> do
            let newSeqId = max seqId currSeqId
            maybe (handleSuccess $ max seqId currSeqId) (handleErr newSeqId) $ checkSeqId seqId currSeqId
          Left e -> do
            pure (Left e, currSeqId)
      where
        handleErr curr e = do
          seqIdFaultHandler e
          pure (Left e, curr)
        handleSuccess s = do
          pure (Right s, s)

    reset mvar newSeqId = do
      modifyMVarMasked mvar (resetSeqId newSeqId)

    -- Do not stomp over error, but overwrite previous seq id
    resetSeqId newSeqId contents = do
      case contents of
        Right s -> pure (Right newSeqId, Nothing)
        Left e -> pure (Left e, Just e)

------------------------------------------------------------------------------
-- | Wrap an 'System.IO.Streams.OutputStream' to give a sequence ID for each element written.
--
-- Example:
--
-- @
-- (outStream', getSeqId) <- 'sequenceIdOutputStream' 1 outStream
-- return $ 'System.IO.Streams.Combinators.contramapM' (addSeqId getSeqId) outStream'
-- @
sequenceIdOutputStream :: Integral s
                       => s                   -- ^ Initial sequence ID
                       -> (s -> a -> b)       -- ^ Transformation function
                       -> OutputStream b      -- ^ 'System.IO.Streams.OutputStream' to count the elements of
                       -> IO (OutputStream a) -- ^ ('IO' 'SequenceId') is the action to run to get the current sequence ID
sequenceIdOutputStream i f = outputFoldM f' i
  where f' seqId bdy = (nextSeqId, f nextSeqId bdy)
          where nextSeqId = incrementSeqId seqId


outputFoldM :: (a -> b -> (a, c))  -- ^ fold function
            -> a                   -- ^ initial seed
            -> OutputStream c      -- ^ output stream
            -> IO (OutputStream b) -- ^ returns a new stream as well as an IO action to fetch the updated seed value.
outputFoldM step initSeqId outStream = do
    ref <- newIORef initSeqId
    Streams.makeOutputStream (wr ref)
  where
    wr _ Nothing    = Streams.write Nothing outStream
    wr ref (Just x) = do
        !accum <- readIORef ref
        let (!accum', !x') = step accum x
        writeIORef ref accum'
        Streams.write (Just x') outStream


------------------------------------------------------------------------------
-- | Same behavior as 'sequenceIdOutputStream' except provides a function to reset the sequence id.
sequenceIdOutputStreamWithReset :: Integral s
                                => s                                                        -- ^ Initial sequence ID
                                -> (s -> a -> b)                                            -- ^ Transformation function
                                -> OutputStream b                                           -- ^ 'System.IO.Streams.OutputStream' to count the elements of
                                -> IO (OutputStream a, s -> IO (Maybe (SequenceIdError s))) -- ^ ('IO' 'SequenceId') is the action to run to get the current sequence ID
sequenceIdOutputStreamWithReset i f = outputFoldMWithReset f' i
  where f' seqId bdy = (nextSeqId, f nextSeqId bdy)
          where nextSeqId = incrementSeqId seqId


outputFoldMWithReset :: forall a b c.
                        (a -> b -> (a, c))  -- ^ fold function
                     -> a                   -- ^ initial seed
                     -> OutputStream c      -- ^ output stream
                     -> IO (OutputStream b, a -> IO (Maybe (SequenceIdError a))) -- ^ returns a new stream as well as an IO action to fetch the updated seed value.
outputFoldMWithReset step initSeqId outStream = do
    ref <- newIORef $ Right initSeqId
    (,reset ref) <$> Streams.makeOutputStream (wr ref)
  where
    wr _ Nothing    = Streams.write Nothing outStream
    wr ref (Just x) = do
        msg <- atomicModifyIORef' ref (updateRef x)
        Streams.write msg outStream
        pure ()

    updateRef :: b -> Either (SequenceIdError a) a -> (Either (SequenceIdError a) a, Maybe c)
    updateRef x contents = do
        case contents of
          Right accum -> do
            let (!accum', !x') = step accum x
            (Right accum', Just x')
          Left e -> do
            (Left e, Nothing)

    reset :: IORef (Either (SequenceIdError a) a) -> a -> IO (Maybe (SequenceIdError a))
    reset ref newSeqId = atomicModifyIORef ref (resetSeqId newSeqId)

    -- Do not stomp over error, but overwrite previous seq id
    resetSeqId newSeqId contents = do
      case contents of
        Right s -> (Right newSeqId, Nothing)
        Left e -> (Left e, Just e)
