{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}

module Core
  (
    Host
  , Command
  , UID
  , CommandBuilder (toCommand)
  , CommandModifier
  , Job (..)
  , numHosts
  , command
  , runJobs
  , modId
  ) where

import System.Process (spawnCommand, waitForProcess)
import Control.Concurrent (threadDelay, forkIO)

import Control.Concurrent.Chan (newChan, writeChan, readChan)

import Data.List (sort, splitAt)

type Host    = String
type Command = String
type UID     = Integer

class CommandBuilder a where
  toCommand :: a -> String

instance CommandBuilder String where
  toCommand s = s

data Job a where
  Job :: CommandBuilder a => Int -> a -> Job a

instance Eq (Job a) where
  Job i1 _ == Job i2 _ = i1 == i2

instance Ord (Job a) where
  Job i1 _ <= Job i2 _ = i1 <= i2

numHosts :: Job a -> Int
numHosts (Job i _) = i

command :: Job a -> String
command (Job _ c) = toCommand c

type CommandModifier a = [Host] -> UID -> Job a -> Job a

modId :: CommandModifier a
modId _ _ j = j

runJobs :: CommandModifier a -> [Host] -> [Job a] -> IO ()
runJobs mod nodes jobs = do
  availableNodes <- newChan
  mapM_ (writeChan availableNodes) nodes
  go 0 availableNodes (sort jobs) []

 where
   go uid chan js@(j:jobs) freeNodes = do
     let hostsNeeded = numHosts j
     if length freeNodes >= hostsNeeded
       then do
         let (alloced, rem) = splitAt hostsNeeded freeNodes
         forkIO $ runJob j alloced uid chan
         go (uid + 1) chan jobs rem
       else do
         -- Blocks until more nodes are available
         avail <- readChan chan
         go uid chan js (avail:freeNodes)

   go uid chan [] freeNodes = do
     if length freeNodes == length nodes
       then return () -- All jobs complete, we are done
       else do
         avail <- readChan chan
         go uid chan [] (avail:freeNodes)

   runJob job nodes uid chan = do
      runSSH_ (head nodes) (command $ mod nodes uid job)
      mapM_ (writeChan chan) nodes

runSSH_ :: Host -> Command -> IO ()
runSSH_ host cmd = do
  let cmd' = "ssh -A " ++ host ++ " " ++ cmd
  print $ "Running: " ++ cmd'
  spawnCommand cmd' >>= waitForProcess
  threadDelay (2 * 10^6)
