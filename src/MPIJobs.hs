module MPIJobs
(
   MPIJob (..)
 , modMPI
) where

import Core
import Data.List (intercalate)

data MPIJob = MPIJob {
    preCommands      :: [String]
  , postCommands     :: [String]
  , createOutputFile :: UID -> String
  , mpiArgs          :: String
  , cmd              :: String
}

-- We assume bash style commands
instance CommandBuilder MPIJob where
  toCommand j =
       unwords (map (++ ";") (preCommands j))
    ++ cmd j
    ++ unwords (map (++ ";") (postCommands j))

modMPI :: CommandModifier MPIJob
modMPI hosts uid (Job n c) = Job n mpiJob'
  where
    outF   = createOutputFile c uid
    -- We modify all pre/post commands to putput to the file
    preCommands'  = map (\x -> x ++ " 2>&1 > " ++ outF) (preCommands c)
    postCommands' = map (\x -> x ++ " 2>&1 > " ++ outF) (postCommands c)

    mpiCmd = "mpiexec -n " ++ show n ++ " -hosts " ++ intercalate "," hosts ++ " " ++ mpiArgs c
    cmd'   = intercalate " " [mpiCmd, cmd c, "2>&1 >", outF]

    mpiJob' = c {
      preCommands  = preCommands'
    , postCommands = postCommands'
    , cmd          = cmd'
    }
