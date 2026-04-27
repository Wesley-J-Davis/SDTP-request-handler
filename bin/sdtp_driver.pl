#!/usr/bin/perl
#
# PROGRAM: sdtp_driver.pl is the operational driver for the SDTP-2
# processing.
# 
# 25Aug10 R. Lucchesi - initial implementation.
# 23Jan16 W. Davis    - adapted for sdtp implementation
# The setting of the options and the module lookup paths will
# be done first using the BEGIN subroutine.  This section of the
# program executes before the rest of the program is even compiled.
# This way, a new path set via the -P option can be used to locate
# the modules to include at compile time while the remainder of the
# program is compiled.


BEGIN {

# Keep track of errors within BEGIN block.

   $die_away = 0;
# Initialize output listing location

   $opt_O = 0;

# This module contains the getopts() subroutine.

   use Getopt::Std;
   use Getopt::Long;

# Get options and arguments

   $num_args=$#ARGV;
   GetOptions  ('E:s',\$opt_E,
		'P:s',\$opt_P,
		'R:s',\$opt_R,
		'O:s',\$opt_O,
		'd:s',\$opt_d,
      'a',\$opt_a,
      'b',\$opt_b,
     	'sched_cnfg:s',\$sched_cnfg,
      'sched_id=s',\$sched_id,
      'sched_synp:s',\$sched_synp,
      'sched_c_dt:s',\$sched_c_dt,
      'sched_dir:s',\$sched_dir,
      'sched_sts_fl:s',\$sched_sts_fl,
      'sched_hs:s',\$sched_hs );

# Processing environment

   $env = "ops";

# The pre-processing configuration file.

   if ( defined( $opt_E ) ) {
      $PREP_CONFIG_FILE = $opt_E;
   } else {
      $PREP_CONFIG_FILE = "DEFAULT";
   }

# Path to directory containing other GEOS DAS programs.
# Directory $GEOSDAS_PATH/bin will be searched for these
# programs.

   if ( defined( $opt_P ) ) { 
      $GEOSDAS_PATH = $opt_P;
   } else {
      $GEOSDAS_PATH = "DEFAULT";
   }

# Location of run-time configuration file.

   if ( defined( $opt_R ) ) { 
      $RUN_CONFIG_FILE = $opt_R;
   } else {
      $RUN_CONFIG_FILE = "DEFAULT";
   }

#  If sdtp_driver.pl is initiated by the scheduler, construct table
# info. for "task_state" table of scheduler

   if ( defined( $sched_id ) )
   {
      $tab_status = 1;
      $tab_argv = "$sched_cnfg, $sched_id, $sched_synp, $sched_c_dt";
      $fl_name = "sdtp";
   }

# ID for the preprocessing run.

$prep_ID = flk;

# Location for output listings

   if ( $opt_O ) { 
      system ("mkdir -p $opt_O");
      if ( -w "$opt_O" ) {
        $listing_file    = "$opt_O/sdtp_${prep_ID}.$$.listing";
        $listing_file_gz    = "$opt_O/sdtp_${prep_ID}.$$.listing_gz";
        print "Standard output redirecting to $listing_file\n";
        open (STDOUT, ">$listing_file");
        open (STDERR, ">&" . STDOUT);
      }
      else {
        print "$0: WARNING: $opt_O is not writable for listing.\n";
      }
   }else{
        $listing_file = "STDOUT"
   }

   if ( ${num_args} < 0 || $#ARGV != -1 ) {
       print STDERR <<'ENDOFHELP';
Usage:

   sdtp_driver.pl [-E Prep_Config] [-P GEOSDAS_Path] [-R Run_Config] [ -O output_location ] [ -d process_date ] [ -t synoptic_time ]

   Normal options and arguments:

   -O output_location
         If this option is specified, output listings (both STDERR and STDOUT) will be
         placed in the directory "output_location."  

   -E Prep_Config
         The full path to the preprocessing configuration file.  This file contains
         parameters needed by the preprocessing control programs. If not given, a
         file named $HOME/$prep_ID/Prep_Config is used.  sdtp_driver.pl exits with an
         error if neither of these files exist.

         The parameters set from this file are

         SDTP_BASE           
            This is the base installation directory of the SDTP-3 software.


   -d process_date
         Date in YYYYMMDD format to process.  If not given, then today's date (in
         terms of GMT) will be processed.

   -P GEOSDAS_Path
         Path to directory containing other GEOS DAS programs.  The path is 
         $GEOSDAS_PATH, where $GEOSDAS_PATH/bin is the directory containing these
         programs.  If -P GEOSDAS_Path is given, then other required programs not 
         found in the directory where this program resides will be obtained from 
         subdirectories in GEOSDAS_Path - the subdirectory structure is assumed 
         to be the same as the operational subdirectory structure.  The default is 
         to use the path to the subdirectory containing this program, which is what 
         should be used in the operational environment.

   -R Run_Config
         Name of file (with path name, if necessary) to read to obtain the 
         run-time (execution) configuration parameters.  sdtp_driver.pl reads this
         file to obtain configuration information at run time.  

         If given, sdtp_driver.pl uses this file.  Otherwise, sdtp_driver.pl looks for a 
         file named "Run_Config" in the user's home directory, then the 
         $GEOSDAS_PATH/bin directory.  $GEOSDAS_PATH is given by the -P option if
         set, or it is taken to be the parent directory of the directory in which this
         script resides.  It is an error if sdtp_driver.pl does not find this file, 
         but in the GEOS DAS production environment, a default Run_Config file is always 
         present in the bin directory.

ENDOFHELP
      $die_away = 1;
   }

# This module locates the full path name to the location of this file.  Variable
# $FindBin::Bin will contain that value.

   use FindBin;

# This module contains the dirname() subroutine.

   use File::Basename;

# If default GEOS DAS path, set path to parent directory of directory where this
# script resides.  

   if ( $GEOSDAS_PATH eq "DEFAULT" ) {
      $GEOSDAS_PATH = dirname( $FindBin::Bin );
   }
# Set name of the bin directory to search for other programs needed by this one.

   $BIN_DIR = "$GEOSDAS_PATH/bin";

# Get the name of the directory where this script resides.  If it is different 
# than BIN_DIR, then this directory will also be included in the list of 
# directories to search for modules and programs.

   $PROGRAM_PATH = $FindBin::Bin;

# Now allow use of any modules in the bin directory, and (if not the same) the
# directory where this program resides.  (The search order is set so that
# the program's directory is searched first, then the bin directory.)

   if ( $PROGRAM_PATH ne $BIN_DIR ) {
      @SEARCH_PATH = ( $PROGRAM_PATH, $BIN_DIR );
   } else {
      @SEARCH_PATH = ( $BIN_DIR );
   }

}	# End BEGIN

# Any reason to exit found during the BEGIN block?

if ( $die_away == 1 ) {
   print "(sdtp_driver.pl) FAILED due to die_away condition"; 
   exit 1;
}

# Include the directories to be searched for required modules.

use lib ( @SEARCH_PATH );

# Set the path to be searched for required programs.

$ENV{'PATH'} = join( ':', @SEARCH_PATH, $ENV{'PATH'} );

# This module contains the extract_config() subroutine.
use Extract_config;

# Archive utilities: gen_archive
use Arch_utils;

# This module contains the z_time(), dec_time() and date8() subroutines.
use Manipulate_time;

# Error logging utilities.
use Err_Log;

# Record FAILED to schedule status file.
use Recd_State;

# This module contains the mkpath() subroutine.

use File::Path;
use File::Copy;

# This module contains the rget() routine.

use Remote_utils;

# This module contains the julian_day subroutine.

use Time::JulianDay;

#Initialize exit status

$exit_stat = 0;

# Write start message to Event Log

err_log (0, "sdtp_driver.pl", "$prep_ID","$env","-1",
        {'err_desc' => "$prep_ID sdtp_driver.pl job has started - Standard output redirecting to $listing_file"});

# Use Prep_Config file under the preprocessing run's directory in the user's home directory
# as the default.

if ( "$PREP_CONFIG_FILE" eq "DEFAULT" ) {
   $PREP_CONFIG_FILE = "$ENV{'HOME'}/$prep_ID/Prep_Config";
}

# Set the date if it was not given.
# If date was not given, adjust time back if we are running 18z after the 00z boundary.

   if ( defined( $opt_d ) ) {
      $process_date = $opt_d; 
   } else {
      ( $process_date, $current_time ) = z_time();
   }

   $err_time = "${process_date}";

# Does the Prep_Config file exist?  If not, die.
if ( ! -e "$PREP_CONFIG_FILE" ) {
    err_log (4, "sdtp_driver.pl", "$err_time","$prep_ID","-1",
	     {'err_desc' => "error $PREP_CONFIG_FILE not found."});
    die "error $PREP_CONFIG_FILE not found.";
}

# Read from Prep_Config environment settings needed by SDTP request handling..

( $SDTP_BASE = extract_config( "SDTP_BASE", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or die "(sdtp_driver.pl) ERROR - can not set SDTP_BASE configuration value\n";
$ENV{'SDTP'} = $SDTP_BASE;

( $SDTP_PYTHON_PATH = extract_config( "PYTHON", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or die "(sdtp_driver.pl) WARNING - can not set SDTP_PYTHON_PATH configuration value\n";

( $SDTP_CERT = extract_config( "CERT", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or die "(sdtp_driver.pl) WARNING - can not set SDTP_CERT configuration value\n";

( $SDTP_KEY = extract_config( "KEY", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or die "(sdtp_driver.pl) WARNING - can not set SDTP_KEY configuration value\n";

# Get Query Data.

( $MAXFILE = extract_config( "MAXFILE", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or print "(sdtp_driver.pl) WARNING - can not set MAXFILE configuration value\n";
( $STREAM = extract_config( "STREAM", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or print "(sdtp_driver.pl) WARNING - can not set STREAM configuration value\n";

# Get output directory

( $SDTP_OUTPUT_BASE = extract_config( "OUTPUT_DIR", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or die "(sdtp_driver.pl) ERROR - can not set SDTP_OUTPUT_BASE configuration value\n";

( $DATA_STAGE = extract_config( "DATA_STAGE", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or print "(sdtp_driver.pl) WARNING - can not set DATA_STAGE configuration value\n";

( $TEST_MODE = extract_config( "TEST_MODE", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
   or print "(sdtp_driver.pl) INFO - TEST_MODE variable not set\n";

$ENV{'PYTHONPATH'} = "${SDTP_PYTHON_PATH}";

$ENV{'PATH'} = join( ':', "${SDTP_BASE}/bin/:/discover/nobackup/projects/gmao/share/dasilva/bin/:$DATA_STAGE", $ENV{'PATH'} );
 do "/usr/share/modules/init/perl";
 module ("purge");

print "source g5_modules. ${GEOSDAS_PATH}/g5_modules_perl_wrapper\n";
{
    local @ARGV = ("${GEOSDAS_PATH}");
    do "g5_modules_perl_wrapper";
}

# If a g5_modules exists in the bin directory, source it.

#**********************#
# Start the processing #
#**********************#

print "Starting SDTP download utility.\n";

module ("list");
print "PYTHONPATH=$ENV{'PYTHONPATH'}\n";
print "PATH=$ENV{'PATH'}\n";

my ($year, $month, $day) = $process_date =~ /(\d{4})(\d{2})(\d{2})/;
print "$year : $month : $day\n";
if ( !$TEST_MODE  ) { 
  $cmd = "python ${SDTP_BASE}/bin/sdtp_download.py --stream $STREAM --maxfile $MAXFILE --cert $SDTP_CERT --key $SDTP_KEY --output-dir $SDTP_OUTPUT_BASE";
}
else { 
  $cmd = "python ${SDTP_BASE}/bin/sdtp_download.py --stream $STREAM --maxfile $MAXFILE --cert $SDTP_CERT --key $SDTP_KEY --output-dir $SDTP_OUTPUT_BASE --test-mode";
}

print "$cmd\n";
$rc=system("$cmd");
print "RETURN CODE=$rc\n";

if ($rc != 0 ) {
    err_log (4, "sdtp_driver.pl", "$err_time","$prep_ID","-1",
	     {'err_desc' => "Error running sdtp_driver.py.  Check listing."});
    recd_state( $fl_name, FAILED, $tab_argv, $sched_dir, $sched_sts_fl );
    die "error running sdtp_driver.py";
}

my $input_dir   = $SDTP_OUTPUT_BASE;
my $output_base = $DATA_STAGE;

opendir(my $dh, $input_dir) or die "Cannot open directory $input_dir: $!";
my @files = grep { -f "$input_dir/$_" } readdir($dh);
closedir($dh);

foreach my $file (@files) {
    # Match the pattern .A + 4 digit year + 3 digit DOY + .
    # Example: MCD43C2N.A2026068.061... -> $1 = 2026, $2 = 068
    if ($file =~ /\.A(\d{4})(\d{3})\./) {
        my $year = $1;
        my $doy  = $2;

        # Construct the output directory path (year / doy)
        my $output_dir = "$output_base/$year/$doy";

        # Create the directory structure if it doesn't exist
        system("mkdir -p $output_dir") ;

        # Construct full paths
        my $source_file = "$input_dir/$file";
        my $dest_file   = "$output_dir/$file";

        # Move the file
        $rc = system("mv $source_file $dest_file");
        if ($rc != 0) {
             err_log (4, "sdtp_driver.pl", "$err_time","$prep_ID","-1",
                 {'err_desc' => "Error running sdtp_driver.py.  Check listing."});
             recd_state( $fl_name, FAILED, $tab_argv, $sched_dir, $sched_sts_fl );
             die "error moving $file to $output_dir";
        } else {
            print "Moved: $file -> $output_dir/\n";
        }
    } else {
        warn "Skipping $file: Does not match the standard .AYYYYDDD. pattern.\n";

    }
}

########################
# Rename output listings
########################

if ( $opt_O ) {
    system ("mv $listing_file $opt_O/sdtp_${prep_ID}.${err_time}.listing");
}

#  If sdtp_driver.pl is initiated by the scheduler and the output file size
# is not zero, write COMPLETE to a task status file.

  if ( defined( $sched_id ) && $tab_status != 0 )
  {
     recd_state( $fl_name, "COMPLETE", $tab_argv, $sched_dir, $sched_sts_fl );
  }

  print "\nFinished.\n";

exit 0;
