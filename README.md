# LANL-model

Some analysis of LANL data (netflow currently). In progress.

Data processing chain:

<ol>
<li> 
SQL is used to generate files <tt>data/proto_features.csv</tt> and <tt>data/port_features_2.csv</tt>
<li>
R is used to generate files <tt>data/box_profiles_20160524_100.Rds</tt> and <tt>data/box_coords.csv</tt>
<li>
PySpark in notebook <tt>data_munging.ipynb</tt> is used to generate file <tt>flows_for_lanl_model_*.csv</tt>
<li>
Full version of the last file with 130M records is downloadable <a href="http://billsdata.net/data/">from here</a>.
</ol>