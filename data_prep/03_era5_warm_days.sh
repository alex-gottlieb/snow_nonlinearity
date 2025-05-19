#!/bin/bash

# Name of the job
#SBATCH --job-name=era5_warm
# Number of compute nodes
#SBATCH --nodes=1
# Number of tasks per node
#SBATCH --ntasks-per-node=1
# Number of CPUs per task
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=4GB
# Walltime (job duration)
#SBATCH --time=36:00:00
# Email address
#SBATCH --mail-user=Alexander.R.Gottlieb.GR@dartmouth.edu
# Email notifications (comma-separated options: BEGIN,END,FAIL)
#SBATCH --mail-type=BEGIN,END,FAIL
# CMIG account
#SBATCH --account=CMIG
#SBATCH --array=1997-2024

mv *.out /dartfs-hpc/rc/lab/C/CMIG/agottlieb/snow_nonlinearity/slurm_out/

source /optnfs/common/miniconda3/etc/profile.d/conda.sh
conda activate arg3

python3 03_era5_warm_days.py $SLURM_ARRAY_TASK_ID


