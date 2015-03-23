use warnings;
use strict;
use Text::CSV_XS;

my $transmart_properties_file = 'transmart-data-migration.properties';
my %properties;

open my $properties_fh, "<:encoding(utf8)", $transmart_properties_file or die $!;
map{ $properties{$1}=$2 while m/(.+)=(.+)/g; }<$properties_fh>; close $properties_fh;

my $csv1 = get_csvfile_object();
my $csv2 = get_csvfile_object();

open my $mapping_file_fh, "<:encoding(utf8)", $properties{'mapping-file'} or die $!;
my @mapping_file_column_labels = $csv1->column_names($csv1->getline($mapping_file_fh));
my %mapping_file_column_values = (); $csv1->bind_columns(\@mapping_file_column_values{@mapping_file_column_labels});

my %mapping_file_hash = ();
while( $csv1->getline_hr($mapping_file_fh) ) {
	my $OC_COLUMN_TYPE = $mapping_file_column_values{'OC_COLUMN_TYPE'};
	my $TM_COLUMN_TYPE = $mapping_file_column_values{'TM_COLUMN_TYPE'};
	my $OC_CONCEPT = $mapping_file_column_values{'OC_CONCEPT'};
	my $TM_CONCEPT_CODE = $mapping_file_column_values{'TM_CONCEPT_CODE'};
	
	#$mapping_file_hash{$OC_COLUMN_TYPE}{$TM_COLUMN_TYPE}{$OC_CONCEPT} = $TM_CONCEPT_CODE;
	$mapping_file_hash{$OC_COLUMN_TYPE}{$OC_CONCEPT} = $TM_CONCEPT_CODE;
}close($mapping_file_fh);

open my $data_file_fh, "<:encoding(utf8)", $properties{'oc-data-file'} or die $!;
my @data_file_column_labels = $csv2->column_names($csv2->getline($data_file_fh));
my %data_file_column_values = (); $csv2->bind_columns(\@data_file_column_values{@data_file_column_labels});

my @tm_observation_fact_headers = ('ENCOUNTER_NUM','PATIENT_NUM','CONCEPT_CD','PROVIDER_ID','START_DATE','MODIFIER_CD','INSTANCE_NUM','VALTYPE_CD','TVAL_CHAR','NVAL_NUM','VALUEFLAG_CD','QUANTITY_NUM','UNITS_CD','END_DATE','LOCATION_CD','OBSERVATION_TEXT','CONFIDENCE_NUM','UPDATE_DATE','DOWNLOAD_DATE','IMPORT_DATE','SOURCESYSTEM_CD','UPLOAD_ID','SAMPLE_CD');
my $write_observation_fact_csv = get_csvfile_object();
open my $tm_upload_file_fh, ">:encoding(utf8)", $properties{'transmart-upload-file-name'} or die $!;
$write_observation_fact_csv->print($tm_upload_file_fh, \@tm_observation_fact_headers);

my @observation_fact_values;
while( $csv2->getline_hr($data_file_fh) ) {
	#print $data_file_column_values{'StudySubjectID'},"\n";
	push(@observation_fact_values, $data_file_column_values{'StudySubjectID'});
	push(@observation_fact_values, $data_file_column_values{'StudySubjectID'});
	my $tm_concept_code = $mapping_file_hash{'HIS_PATHTNMTUM_E8_1_C10'}{$data_file_column_values{'HIS_PATHTNMTUM_E8_1_C10'}};
	push(@observation_fact_values, $tm_concept_code);
	push(@observation_fact_values, '@');
	push(@observation_fact_values, '2015-03-20');
	push(@observation_fact_values, '@');
	push(@observation_fact_values, '');
	push(@observation_fact_values, $mapping_file_column_values{'TM_COLUMN_TYPE'});
	if( $mapping_file_column_values{'TM_COLUMN_TYPE'} eq 'T' ) {
		push(@observation_fact_values, $tm_concept_code);
		push(@observation_fact_values, '');
	}else {
		push(@observation_fact_values, 'E');
		push(@observation_fact_values, $tm_concept_code);		
	}
	push(@observation_fact_values, '@', '', '', '2015-03-20', '@', '', '', '2015-03-20', '2015-03-20', '2015-03-20', 'OPENCLINICA', 1, 1);
	$write_observation_fact_csv->print($tm_upload_file_fh, \@observation_fact_values);
	undef @observation_fact_values;
	#$mapping_file_column_values{'TM_COLUMN_TYPE'} == 'T' ? push(@observation_fact_values, $tm_concept_code) : push(@observation_fact_values, '');
	#push(@observation_fact_values, $data_file_column_values{'Study Subject ID'});
	#push(@observation_fact_values, $data_file_column_values{'Study Subject ID'});
}close($data_file_fh); close($tm_upload_file_fh);

sub get_csvfile_object {
	my $csv = Text::CSV_XS->new( { eol => $/ } ) 
	or die "Cannot use CSV: ".Text::CSV_XS->error_diag();
	
	return $csv;
}