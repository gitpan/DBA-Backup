package DBA::Backup;


=head1 NAME

DBA::Backup - Core module for managing automated database backups.

=head1 SYNOPSIS

NOTICE! This is currently a broken partial port from the origal working
MySQL specific module. I hope to have the port finished and a functional
version uploaded soon. Email me or the list for more information.

The mailing list for the DBA modules is perl-dba@fini.net. See
http://lists.fini.net/mailman/listinfo/perl-dba to subscribe.

  use DBA::Backup;
  
  my $dba = new DBA::Backup(%params);
  die "Can't initiate backups: $dba" unless ref $dba;
  
  $dba->run(%conf_overides);
  $dba->log_messages();
  $dba->send_email_notification();

=cut

use 5.008003;
use strict;
use warnings;

use Sys::Hostname;  # provides hostname()
use File::Copy qw/move/;
use File::Path qw/rmtree/;
use Mail::Sendmail; # for sending mail notifications
use YAML qw(LoadFile); # config file processing


our $VERSION = '0.1';

# prevent this module from granting any privilege to all (other users)
umask(0117);

=head1 new()

Create new DBA::Backup object. Use this object to initiate backups.

OPTIONS:

CONF_FILE:	Location of configuration file to use. Default is
/etc/dba-backup.yml. Please keep in mind that conf files for any specific
servers to be backup will need to be in the same location.

LOG_FILE:	Location to write process log file.

BACKUP:	If true will force full database backups.

ADD_DATABASES:	Specify additional databases to be backed up. ** broken

=cut

sub new {
	my $class   = shift;
	my %params = ref($_[0]) eq 'HASH' ? %{$_[0]} : @_;
	$params{CONF_FILE} ||= '/etc/dba-backup.yml';
	
	# exits with usage statement if the config file is not valid
	_is_config_file_valid($params{CONF_FILE}) or usage($params{CONF_FILE});
	
	# Read the YAML formatted configuration file
	my $HR_conf = LoadFile($params{CONF_FILE})
		or return ("Problem reading conf file $params{CONF_FILE}");
	
	# now lets modify for certain passed parameters
	$HR_conf->{LOG_FILE} = $params{LOG_FILE} if $params{LOG_FILE};
	my $cur_day = substr(localtime,0,3);
	$HR_conf->{backup}{days} = $cur_day if $params{BACKUP};
	if ($params{ADD_DATABASES}) {
		my $AR_dbs = $HR_conf->{backup}{databases};
		foreach my $db (split(/ ?, ?/,$params{ADD_DATABASES})) {
			push(@{$AR_dbs},$db) unless grep (/$db/, @{$AR_dbs});
		} # for each db to add
	} # if backing up additional databases 
	
	# this opens the log file for writing, turns off the buffer and redirects
	# STDERR to the log
	$HR_conf->{LOG} = _open_log_file($HR_conf->{LOG_FILE});
	
	# Stores the name of the current config file in the object
	$HR_conf->{backup_params}{CONF_FILE} = $params{CONF_FILE};
	$HR_conf->{db_connect}{HOSTNAME} = Sys::Hostname::hostname();
	
	# list of database servers (limit 1/type ATM) to backup
	my @rdbms = grep(/\w+_backup/, keys %{$HR_conf});
	$HR_conf->{backup_servers} = \@rdbms;
	
	return bless $HR_conf, $class;
} # end new()


sub _open_log_file {
	my $logfile = shift;
	
	open(my $LOG,"> $logfile") or die "Can't write logfile $logfile: $!";
	
	# sig warn and die handlers to write errors to log before sending to STDERR
	sub catch_sig {
		my $signame = shift;
		my $msg = shift;
		print $LOG "$signame: $msg\n";
		die "$signame: $msg\n";
	} # catch_sig
	
	$SIG{INT} = \&catch_sig;
	$SIG{QUIT} = \&catch_sig;
	$SIG{warn} = \&catch_sig;
	$SIG{die} = \&catch_sig;
	
	warn "Testing sig handlers";
	
	$|++;
	return $LOG;
} # _open_log_file

=head1 usage()

    Prints an usage message for the program on the screen and then exits.

=cut 

sub usage {
	my $file = shift;
	die "Usage: $0 /path/backup.cnf\n"
		. "Please make sure you specified a config file with proper format."
		. "$file provided.\n"; 
}


sub _is_config_file_valid {
	my $file = shift;
	return 0 unless $file;
	
	# if file name does not start with ./ or /, then append ./
#	unless ($file =~ m{^[./]} ) {
#		$_[0] = "./$_[0]";
#	}
	
	unless (-f $file && -r $file) {
		return 0;
	} # unless file exists and is reabable
	
	return 1;
}


##
## The above section sets up and handles configuration parsing
## The section below actually manages the backups
##


=head1 run()

    This is where most of the work in the program is done.
    It logs some messages to the log file and invokes the subroutines
    for database backup and log backup and rotation.

=cut

sub run {
	my $self = shift;
	my $time = localtime();
	
#	warn "Starting log";
	
	# Greeting message
	print $self->{LOG} <<LOG;
	
*** DBA::Backup process started [$time] ***

LOG
	
	# manage backups for each server
	foreach my $server (@{$HR_conf->{backup_servers}}) {
		# require and import server module
		my $server_pkg = "DBA::Backup::$server";
		require $server_pkg && import $server_pkg;
		# create dba backup server object
		my $dbs = new $server_pkg($self);
		
		my $host = $self->{db_connect}{HOSTNAME};
		my $log_dir = $self->{backup_params}{LOG_DIR};
		my $dump_dir = $self->{backup_params}{DUMP_DIR};
		my $dump_copies = $self->{backup_params}{DUMP_COPIES};
		my $conf_file = $self->{backup_params}{CONF_FILE};
		$time = localtime();
		
		
#		warn "Starting log";
	
		# Greeting message
		print $self->{LOG} <<LOG;
	
$server backup parameters:
	Host and mysql server: $host
	Log dir: $log_dir
	Dump dir: $dump_dir
	Dumps to keep: $dump_copies
	Backup config file: $conf_file
	
*Tidying up dump dirs*
LOG
		
		
#		warn "_tidy_dump_dirs";
		# clean up dump dirs
		$self->_tidy_dump_dirs();
		
		# check the disk space on the dump directory 
		# for now, use the unix df command, later use perl equivalent
		my $disk_usage = `df '$dump_dir'`; 
		print $self->{LOG} "\n\n*Disk space report for $dump_dir*\n$disk_usage";
		
		# check the list of currently running mysql queries
		print $self->{LOG} "\n\n*Current processlist*\n";
#		warn "_get_process_list";
		print $self->{LOG} $self->_get_process_list();
		
		# rotate the logs.  most text log files need to be renamed manually
		# before the "flush logs" command is issued to mysqld
		# we rotate logs daily (well, every time script is run)
		print $self->{LOG} "\n\n*Rotating logs*\n";
#		warn "_rotate_general_query_log";
		$self->_rotate_general_query_log();
#		warn "_rotate_slow_query_log";
		$self->_rotate_slow_query_log();
#		warn "_cycle_bin_logs";
		$self->_cycle_bin_logs();
#		warn "_rotate_error_log";
		$self->_rotate_error_log();
		
		# Backup the databases only if today is the day which is 
		# specified in the config file
		my $cur_day = substr(localtime,0,3);
		my @backup_days = split(/ ?, ?/,$self->{backup}{days});
		
		if (grep(/$cur_day/i, @backup_days)) {
			print $self->{LOG} "\n\n*Starting dumps*\n";
#			warn '_backup_databases';
			my $b_errs = $self->_backup_databases();
			
			# if there were no problems backing up dbs, rotate the dump dirs
			if (not $b_errs) {
				print $self->{LOG} "\n\n*Rotating dump dirs*\n";
#				warn '_rotate_dump_dirs';
				$self->_rotate_dump_dirs();
			} # if no backup errors
		} # if today is a database backup day
		
	} # for each server to backup
	
	# Goodbye message
	$time = localtime();
	print $self->{LOG} "\n\n*** DBA::Backup finished [$time] ***\n\n";
	
} # end run()


=head1 _test_create_dirs

	Test for the existence and writeability of specified directories.
	If the directories do not exist, attempt to create them.  If unable
	to create writeable directories, fail with error.

=cut

sub _test_create_dirs {
	my $self = shift;
	
	# check the existence and proper permissions on all dirs
	foreach my $dir (@_) {
		# if it doesn't exist, create it
		unless (-d $dir) {
			print $self->{LOG} "Directory $dir does not exist, creating it...\n";
			my $mask = umask;
			umask(0007);
			unless (mkdir($dir) and -d $dir) {
				$self->error("Cannot create $dir.\n");
			} # unless dir created
			umask($mask);
		} # if directory doesn't exist
		# check that we can write to it
		unless (-w $dir) {
			$self->error("Directory $dir is not writable by the user running $0\n");
		} # if directory isn't writable
	} # foreach directory to be tested

} # end _test_create_dirs()


=head1 _rotate_dump_dirs()

	The dump directories contain output from both the full, weekly mysql
	dump as well as the incremental binary update logs that follow the
	dump (possibly multiple binlogs per day).  Rotate these directory 
	names to conform to convention:

	  [dump_dir]/00/  - most recent dump
	  [dump_dir]/01/   - next most recent
	  ...
	  [dump_dir]/_NN/	- oldest

	Where N is [dump_copies] - 1 (in the config file).  [dump_dir]/new/
	is a temporary directory created from _backup_databases.  This will
	be renamed 00/, 00/ will be renamed 01/, and so on.

=cut

sub _rotate_dump_dirs {
	my $self = shift;
	
	my $dump_root = $self->{backup_params}{DUMP_DIR};
	my $max_old   = $self->{backup_params}{DUMP_COPIES} -1;
	$max_old = 0 if $max_old < 0;

	# grab list of files/dirs within dump_root - @dump_root_files
	# create hash of dirs we care about w/in dump_root - %dump_root_hash
	opendir(DIR, $dump_root) 
		or $self->error("Cannot get listing of files in $dump_root\n");
	my @dump_dirs = grep {-d "$dump_root/$_"} readdir(DIR);
	closedir(DIR);
	my %dump_root_hash = ();
	foreach my $dir (@dump_dirs) {
		$dump_root_hash{$dir} = 1 if $dir =~ /^\d+$/;
		$dump_root_hash{$dir} = 1 if $dir =~ /^00$/;
		$dump_root_hash{$dir} = 1 if $dir =~ /^new$/;
	} # for each dump root file

	# prepare instructions on how to rename directories, and in what order
	# do not rename a dir unless it "needs" to be renamed
	
	# this seems wicked kludgy, but I want to understand reasoning before I
	# throw away or improve - spq
	my @dir_order;
	my %dir_map;
	if (exists $dump_root_hash{'new'}) {
		push @dir_order, 'new';
		$dir_map{'new'} = '00';
		
		if (exists $dump_root_hash{'00'}) {
			push @dir_order, '00';
			$dir_map{'00'} = '01';
			
			if ($max_old) {
				foreach my $idx (1 .. $max_old) {
					my $name = sprintf("%02d", $idx);
					last unless exists $dump_root_hash{$name};
					push @dir_order, $name;
					$dir_map{$name} = sprintf("%02d", $idx+1);
				} # foreach archival iteration
			} # if we're keeping archival copies
		} # if there is a current directory as well
	} # if there is a new directory
	
	if (@dir_order) {
		print $self->{LOG} "The following dump dirs will be renamed: "
			. join(", ", @dir_order) . ".\n";
	} # if there are directories to rename
	else {
		print $self->{LOG} "No dump dirs will be renamed.\n";
	} # else note there are none

	# rotate names of the dump dirs we want to keep
	foreach my $old_dname (reverse @dir_order) {
		my $new_dname = $dir_map{$old_dname};
		next if $old_dname eq $new_dname;
		next unless (-d "$dump_root/$old_dname");
		next if (-f "$dump_root/$new_dname");
		print $self->{LOG} "Renaming dump dir $old_dname/ to $new_dname/ in $dump_root/ ...\n";
		File::Copy::move("$dump_root/$old_dname", "$dump_root/$new_dname") 
			or $self->error("Cannot rename $old_dname/ to $new_dname/\n");
	} # for each directory to rename

	# delete oldest dump dir if it exceeds the specified number of copies
	# can't this just be made a delete above if last or such?
	my $oldest_dir = $dump_root . '/' . sprintf("%02d", $max_old+1);
	if (-d $oldest_dir) {
		print $self->{LOG} "Deleting $oldest_dir/ ...\n";
		eval { File::Path::rmtree($oldest_dir) };
		$self->error("Cannot delete $oldest_dir/ - $@\n") if ($@);
		$self->error("$oldest_dir/ deleted, but still exists!\n") if (-d $oldest_dir);
	} # delete past-max oldest archive

} # end _rotate_dump_dirs


=head1 _tidy_dump_dirs()

	The dump directories contain output from both the full, weekly mysql
	dump as well as the incremental binary update logs that follow the
	dump (possibly multiple binlogs per day).  Sometimes a user might
	delete a directory between backup runs (particularly if it has bad
	dumps).

	This function is intended to be run before backups start.  It will
	Attempt to make directory names to conform to convention:

	  [dump_dir]/00/  - most recent dump
	  [dump_dir]/01/   - next most recent
	  ...
	  [dump_dir]/NN/	- oldest

	If there are missing directories, _tidy_dump_dirs will create a
	directory to take its place, such that 00/ should always exist
	and there should be no gaps in the numbering of old directories.  In
	other words, N+1 should be the total number of directories in [dump_dir].

	If there are no gaps to begin with, _tidy_dump_dirs does not rename
	anything.

	This function will also delete any xx directories that exceed the
	[dump_copies] config variable.

	It will never touch [dump_dir]/new/.  It will never modify the contents
	of any of these subdirectories (unless its deleting the whole subdir).

	It will create [dump_dir] and [dump_dir]/00/ if they do not exist.

=cut

# is this routine doing redundant work with above?!?!
sub _tidy_dump_dirs {
	my $self = shift;
	
	my $dump_copies = $self->{backup_params}{DUMP_COPIES};
	my $dump_root = $self->{backup_params}{DUMP_DIR};
	$self->_test_create_dirs($dump_root);
	
	# grab list of files/dirs within dump_root - @dump_root_files
	# create hash of dirs we care about w/in dump_root - %dump_root_hash
	opendir(DIR, $dump_root) 
		or $self->error("Cannot get listing of files in $dump_root\n");
	my @dump_dirs = grep {-d "$dump_root/$_"} readdir(DIR);
	closedir(DIR);
	my %dump_root_hash;
	foreach my $dump_dir (@dump_dirs) {
		$dump_root_hash{$dump_dir} = 1 if $dump_dir =~ /^\d+$/;
		$dump_root_hash{$dump_dir} = 1 if $dump_dir =~ /^00$/;
	} # for each dump directory
	# (the next line requires that [dump_copies] is <= 100) # huh?! why? - spq
	my @dump_root_dirs = sort keys %dump_root_hash;
	
	# prepare instructions on how to rename directories, and in what order
	# also prep instructions on which directories to delete (in case user 
	# has reduced [dump_copies] in the config file since the last time
	# this script was run)
	my %dir_map;
	my @ren_queue;
	my @del_queue;
	my $idx=0;
	foreach my $dir (@dump_root_dirs) {
		if ($idx < $dump_copies) {
			$dir_map{$dir} = sprintf("%02d", $idx);
			push @ren_queue, $dir 
				unless ($dir eq $dir_map{$dir}) or ($dir eq '00');
		} # if dump dir < max copies
		else {
			push @del_queue, $dir;
		} # else prepare delete queue
		$idx++;
	} # foreach dump dir
	
	$dir_map{$dump_root_dirs[0]} = '00' if @dump_root_dirs;
	print $self->{LOG} "The following dump dirs will be renamed: "
		. join(", ", @ren_queue) . ".\n" if @ren_queue;
	print $self->{LOG} "The following dump dirs will be deleted: "
		. join(", ", @del_queue) . ".\n" if @del_queue;
	print $self->{LOG} "Dump dirs look good, not much to tidy up\n"
		if not @ren_queue and not @del_queue;
	
	# shuffle names of the dump dirs
	foreach my $old_dname (@ren_queue) {
		my $new_dname = $dir_map{$old_dname};
		next if $old_dname eq $new_dname;
		next unless (-d "$dump_root/$old_dname");
		next if (-f "$dump_root/$new_dname");
		print $self->{LOG} "Renaming dump dir $old_dname/ to $new_dname/ in "
			. "$dump_root/ ...\n";
		File::Copy::move("$dump_root/$old_dname", "$dump_root/$new_dname") 
			or $self->error("Cannot rename $old_dname/ to $new_dname/\n");
	} # foreach old name?
	
	# delete excess dump dirs
	foreach my $dname (@del_queue) {
		$dname = "$dump_root/$dname";
		print $self->{LOG} "Deleting $dname/ (exceeds dump_copies=$dump_copies) ...\n";
		eval { File::Path::rmtree($dname) };
		$self->error("Cannot delete $dname/ - $@\n") if ($@);
		$self->error("$dname/ deleted, but still exists!\n") if (-d $dname);
	} # for each excess dir to delete
	
	# if not @dump_root_files, create a 00/ dir
	$self->_test_create_dirs("$dump_root/00");
	
} # end _tidy_dump_dirs


=head1 error()

	Logs all the errors so far to a log file then 
	sends an email and exits.

=cut

sub error {
	my $self = shift;
	my $message = shift;
	print $self->{LOG} $message;
	$self->log_messages();
	$self->send_email_notification();
	exit 1;
}


=head1 send_email_notification()

	Sends the data from the 00 run of the program 
	which gets stored in the log file by email. The exact 
	behaviour for this subroutine is controlled by the 
	varibles in [mail-setup] section in the config file

=cut

sub send_email_notification {
	my $self = shift;
#	warn "self? $self";
	
	# Email notifications can be turned off although this
	# is usually not a good idea
	my $notify = $self->{mail_setup}{mail_notification};
	return unless $notify =~ /yes/i;
	
	# send LOG by mail
	# get the varibles below from a config file
	my $hostname = $self->{db_connect}{HOSTNAME};
	my $subject  = "MySQL dump log from $hostname at " . localtime; 
	
	my $to = join(', ', @{$self->{mail_setup}{mail_to}});
	my $cc = join(', ', @{$self->{mail_setup}{mail_cc}});
	
	sendmail(To  => $to,
		 CC      => $cc,
		 Subject => $subject,
		 From    => $self->{mail_setup}{mail_from},
		 Message => (join '', ''), # we no longer store @LOG - what here?
		 Server  => $self->{mail_setup}{mail_server},
		 delay   => 1,
		 retries => 3 ); 

} # end send_email_notification()


1;


=head1 INSTALLATION

To install this module type the following:

   perl Makefile.PL
   make
   make test
   make install

=head1 DEPENDENCIES

This module requires these other modules and libraries:

  Mail::Sendmail # if you want email reports
  YAML
  Sys::Hostname
  File::Copy
  File::Path


=head1 DESCRIPTION

Manages rotation of mysql database logs and database backups. Reads information
on which databases to back up on what days fo the week from the configuration
file. If no file is specified, it will look for one at /etc/mysql-backup.conf.
If no configuration file is found program will exit with a failure.

This program assumes a MySQL 4.0.x server that is at least 4.0.10.
It will likely work with current 1.23.xx server, but that has not been tested.
Please let the maintainers know if you use this tool succesfully with other
versions of MySQL or Perl so we can note what systems it works with.

The expected usage of this program is for it to be run daily by a cron job,
at a time of day convienient to have the backups occur. This program uses the
administrative tools provided with MySQL (mysqladmin and mysqldump) as well
as gzip for compression of backups.

Every time this program is run it will flush the MySQL logs. The binary update
log will be moved into /path/to/dump/dir/00. Error log and slow query log files
are rotated only if they exceeded the size limit specified in the confguration
file.

If it is run on a day when full database backups are specified, then
all databases specified in the config file are dumped and
written to the directory specified in dump_dir variable in the config
file.  If there are no problems with this operation, previous full backups
from dump_dir/00 are moved to directory dump_dir/01 and all the
files in dump_dir/01 (full database backups and log files) are deleted
from it or moved to dump_dir/02 etc. to the archival depth specified in the
config file. This way there always [dump_copies] full database backups - 
one in 00/ and [dump_copies]-1 in the xx directories.

Detailed information about the different configuration parameters
can be found in the comments in the configuration file

log-slow-queries
log-long-format
log-bin

=head1 OPTIONS


=over 4

=item B<logfile>

Filename for logging backup proceedure. Overrides conf file.

=item B<add_databases>

Additional databases to back up. These will be backed up I<in addation> to any
databases specified in the conf file. B<Note> - this adds databases to the list
of those to be backed up. If the program is being run on a day when database
backups are not scheduled, the extra databases specified will B<not> be backed
up.

=item B<backup>

If present this option forces full database backups to be done, even if not
normally scheduled.

=item B<help>

Outputs this help file.

=item B<d>

** NOT IMPLIMENTED **

Turn on debugging. Optionally takes a filename to store debugging and any error
messages to.

=item B<v>

** NOT IMPLIMENTED **

Increases debugging vebosity. If three or more v's provided (-v -v -v)
than program will exit on warnings.

=back

=head1 TO DO

Impliment debugging output options.

Streamline config process - can we avoid using multiple config files?

Support multiple servers of the same type.

=head1 HISTORY

=over 8

=item 0.1

Partial port from original MySQL specific version.

=back



=head1 SEE ALSO

The mailing list for the DBA modules is perl-dba@fini.net. See
http://lists.fini.net/mailman/listinfo/perl-dba to subscribe.

dba-backup.yml

=head1 AUTHOR

Sean P. Quinlan, E<lt>gilant@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2004 by Sean P. Quinlan

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.3 or,
at your option, any later version of Perl 5 you may have available.

=cut

