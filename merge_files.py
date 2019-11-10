#!/usr/bin/env python2.7

import os
import glob
import pandas as pd
import argparse
import time
import datetime

# Create ArgumentParser object.  Add detector ids as optional argument from the command line.
parser = argparse.ArgumentParser(description = 'Merge emorpho photon detection data files from different \
                                                detectors into combined files containing 30 minutes of events.')    
parser.add_argument('--detector', type=str, nargs='*', action='store', default=[],
                    help="Choose from which detectors you want to merge data.  Input as many detector ids \
                    as you want after the flag. ex: '--detector 2545 2963 2721' \
                    If no input, all available data will be merged. ")

# Parse and retrieve arguments.
args = parser.parse_args()
detector_ids = args.detector

timer_start = time.time() # Start timer to measure program run time.

# If detector ids were input, create a list of filenames containing those ids. 
if detector_ids:
    filenames = []
    for i in detector_ids:
        path = './eRC' + i + '*.txt'
        filenames.extend(glob.glob(path))

else:
    # If no detector ids were input, create a list of all filenames in the directory of the form 'eRC*.txt'.
    filenames = glob.glob('./eRC*.txt')
    # Return a message if there are no files of this format found in the directory.
    if not filenames:
        print 'Did not find files to merge'
        
    # Get a list of all detector ids in the list of filenames.
    for i in range(len(filenames)):
        fname = filenames[i]
        detector_id = fname.replace('./eRC','').split('_',1)[0]
        if detector_id not in detector_ids:
            detector_ids.append(detector_id)

filenames.sort() # Arrange the list of filenames in sequential order.
        
date = os.path.basename(os.getcwd()) # Get date of files from the directory name.

# Create a subdirectory for the merged files, unless it already exists.
try: 
    os.mkdir('./MergedFiles_' + date)
except: 
    pass

# Create list of lists of filenames by detector: [[filenames for detector 1], [filenames for detector 2], ...]
files_by_detector = []
for i in detector_ids:
    files_by_detector.append([x for x in filenames if i in x])

# Find the first start time, and add 30 minutes to set the first end time.
start_time = []
for i in range(len(detector_ids)):
    filename = files_by_detector[i][0]
    file = pd.read_csv(filename, sep='\t')
    start_time.append(file['Time'].iloc[0])    
start_time = min(start_time)
end_time = start_time + 1800

def read_in_file(i): # Read in file and create a dataframe from the data within the specified time frame.
    File = files_by_detector[i][0] # Get the first filename from the list of files for detector i.
    File = pd.read_csv(File, sep='\t')
    end = File['Time'].iloc[-1] # Find end time of file.
    # Return data between start time and end time as a new dataframe.
    data_in_range = File.loc[(File['Time'] >= start_time) & (File['Time'] < end_time)]
    return data_in_range, end
    
def process_data(i, data_in_range): # Add the dataframe to a list to be concatenated into the merged file.
    data_in_range.insert(0, 'Detector', detector_ids[i]) # Create a new column and fill it with the detector id.

    if frames_to_concatenate == []:
        # If there is no data in frames_to_concatenate, add the dataframe to the list.
        frames_to_concatenate.append(data_in_range)
    else:
        # Otherwise, check to make sure the dataframe is not already in the list. The file that contains 
        # the endpoint of the specified time frame will remain in the list of files to scan because 
        # it also contains data within the next time frame, so it will be scanned again. 
        # This triggers a break in the loop to move to the next detector.
        if data_in_range.iloc[0].equals(frames_to_concatenate[-1].iloc[0]) == False:
            frames_to_concatenate.append(data_in_range)
        else:
            return True
        
def scan_files(): # Scan files from each detector and add data as dataframes to frames_to_concatenate.
    for i in range(len(detector_ids)): # Iterate over detectors.
        
        while len(files_by_detector[i]) > 0: # End loop when there are no more files to be scanned.
            data_in_range, end = read_in_file(i)

            if data_in_range.empty == False:
                # If the file contains data within the specified time frame, add it to frames_to_concatenate.
                processed = process_data(i, data_in_range)
                if processed == True:
                    break

                if end < end_time:
                    # If the entire file is within the specified time frame, 
                    # remove that filename from the list of files to scan.
                    files_by_detector[i].remove(files_by_detector[i][0])
            else:
                # If the file does not contain data within the specified time frame, print an error message 
                # and move on to the next detector.
                print 'no data in range in this file'
                break
                
def concat_data(): # Concatenate the dataframes (if necessary) into the merged file.
    if len(frames_to_concatenate) > 1:
        # If there is more than one dataframe in the list, concatenate them into the merged file.
        data = pd.concat(frames_to_concatenate)
    else:
        # If there is just one, that dataframe becomes the merged file.
        data = frames_to_concatenate[0]
    return data
    
def save_file(): # Save the merged file, named by start time and detector(s), in the subdirectory MergedFiles.
    detectors = data['Detector'].unique() # Get all unique detector ids in the merged file
    separator = '_eRC'
    detector_names = separator.join(detectors)
    timestamp = datetime.datetime.fromtimestamp(start_time).strftime("%y%m%d_%H%M%S") # Start time of file.
    path = './MergedFiles_' + date + '/'
    filename = timestamp + '_eRC' + detector_names + '.txt'
    data.to_csv(os.path.join(path,filename), sep='\t', index=False, header=True)

# Run this loop until all the data from every file in the filenames list has been copied to a merged file.
while any(files_by_detector):
    frames_to_concatenate = [] # Create empty list for dataframes which will be combined into merged file.
    
    scan_files() # Scan files from each detector and add data as dataframes to frames_to_concatenate.
    
    data = concat_data() # Concatenate the dataframes into the merged file.           
    data = data.sort_values(by=['Time']) # Sort the data so that events are in temporal order.
    save_file() # Save the file to a tab separated text file.
    
    # Increment the start and end time by 30 minutes before iterating over the loop again.
    start_time += 1800
    end_time += 1800

# Stop the timer and print the run time of the program.
timer_stop = time.time()
run_time = timer_stop - timer_start
print 'scan completed in', run_time, 'seconds', '\n'