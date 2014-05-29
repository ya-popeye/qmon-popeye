#!/usr/bin/perl

# use File::Touch;

sub getfs($) {
	my $base = shift;
	$base ||= "/storage";

	open my $f, '/proc/mounts' or die "cannot read /proc/mounts";
	my @points;
	while (<$f>) {
		my ($dev, $point, $fs, $opts) = split(' ', $_, 4);
		$opts =~ /rw/ or next;
		$dev =~ /^\/dev\// or next;
		$point =~ /^$base/ or next;

		push @points, $point;
	}
	close $f;

	return \@points;
}

sub getperc() {
	open my $f, 'df -P |' or die "cannot run df";
	my %h;

	while (<$f>) {
		chomp;
		if ($_ =~ /([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+\%)[ ]+([^ ]+)/) {
			my $point = $6;
			my $perc  = $5;
#			print $point, " ", $perc, "\n";
			$h{$point} = $perc;
		}
	}

	close $f;

	return \%h;
}

sub cleanup($$) {
	my $point = shift;
	my $count = shift;
	print "cleanup $point\n";

	opendir my $dh, $point."/kafka" or die "cannot open $point/kafka";

	foreach (readdir($dh)) {
		my $entry = $point."/kafka/".$_;
		if (-d $entry) {
			print "cleanup $entry\n";
			my @files = glob "$entry/*.log $entry/*.index";
			my $skip_dir = 0;
			if (scalar(@files) < 5) {
				$skip_dir = 1;
			}
			foreach (@files) {
				my $file = $_;
				my $epoch_timestamp = (stat($file))[9];
				if ($epoch_timestamp < 100) {
					$skip_dir = 1;
					last;
				}
			}
			if ($skip_dir == 1) {
				print "skip $entry\n";
				next;
			}
			@files = sort @files;
			@files = splice(@files, 0, 10);
			print join("\n", @files)."\n";
			utime(1, 1, @files);
		}
	}
	
	closedir $dh;
}

my $base  = "/storage";
my $list  = getfs($base);
my $bound = 90;
my $count = 10;
my $h     = getperc();

foreach (@$list) {
	my $point = $_;
#	print "check $point\n";
	if (exists($h->{$point}) && $h->{$point} > $bound) {
		print "$point $h->{$point}\n";
		cleanup($point, $count)
	}
}

