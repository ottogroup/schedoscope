in = LOAD '${env}_schedoscope_example_osm_processed.nodes' using org.apache.hcatalog.pig.HCatLoader();
out = STORE in INTO '${output_table}' using org.apache.hcatalog.pig.HCatStorer();
