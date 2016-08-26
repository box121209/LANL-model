
---------------------------------------------------------------------------
-- Use the  PigStorage function to load the netflow file into the 'flows'
-- bag as an array of records.
-- Input: (time, duration, src, src_pt, dst, dst_pt, proto, pkts, bytes) 

-- flows = LOAD '/Users/wmoxbury/data/LANL/flows.txt' USING PigStorage(',') -- for real
flows = LOAD '/Users/wmoxbury/data/LANL/smallflows.txt' USING PigStorage(',') -- for dev
      	AS (time:int,
	    duration:int,
	    src:chararray,
	    src_pt:chararray,
	    dst:chararray,
	    dst_pt:chararray,
	    proto:int,
	    pkts:int,
	    bytes:int);

-- inspect a random record:
\i flows

---------------------------------------------------------------------------
-- Count flows:

flows_group = GROUP flows ALL;
flow_count = FOREACH flows_group GENERATE COUNT($1);
\d flow_count

-- (129977412)

---------------------------------------------------------------------------
-- Time range:

time_range = FOREACH flows_group GENERATE MIN(flows.time), MAX(flows.time);
\d time_range

-- Total bags proactively spilled: 156
-- Total records proactively spilled: 31720188
-- (1,3126928)
-- NOTE: 3126928 secs = 36.19 days

---------------------------------------------------------------------------
-- Count flows by protocol...

proto_group = GROUP flows BY proto;

-- either:
proto_count = FOREACH proto_group GENERATE group AS proto, COUNT(flows);
-- or equivalently:
proto_count = FOREACH proto_group GENERATE flatten($0), COUNT($1);

\d proto_count

-- Total bags proactively spilled: 0
-- Total records proactively spilled: 0
-- (6,116550579)
-- (1,3321625)
-- (17,10104702)
-- (41,506)

---------------------------------------------------------------------------
-- Ranges by protocol:

proto_ranges = FOREACH proto_group 
               GENERATE flatten($0) AS proto,
	       	      	MIN(flows.duration) AS duration_min,
		      	MAX(flows.duration) AS duration_max,
	     	      	MIN(flows.pkts) AS pkts_min,
		      	MAX(flows.pkts) AS pkts_max,
	     	      	MIN(flows.bytes) AS bytes_min,
		      	MAX(flows.bytes) AS bytes_max;		      
\d proto_ranges

-- Total bags proactively spilled: 1092
-- Total records proactively spilled: 495351273
-- (1,0,71,1,4464,46,374976)
-- (6,0,76,1,2471188,46,2147133532)
-- (17,0,72,1,600678,46,872030312)
-- (41,0,64,1,38,92,3496)

---------------------------------------------------------------------------
-- Number of distinct computers:

src = FOREACH flows GENERATE src AS cmp;
dst = FOREACH flows GENERATE dst AS cmp;
comp_all = UNION src, dst;

-- (or apply DISTINCT to each before the UNION)
-- followed by:

comp = DISTINCT comp_all;
comp_group = GROUP comp ALL;
comp_count = FOREACH comp_group GENERATE COUNT(comp.$0);

\i comp
\d comp_count

-- Total bags proactively spilled: 0
-- Total records proactively spilled: 0
-- (12027)

---------------------------------------------------------------------------
-- Table of per-computer flow- and byte-counts:

--------------------------------
-- Method 1:
--
-- group all flows by src/dst using UNION;
-- but note this will double the size of the data;
-- fails on the full data set.

flows_by_src = GROUP flows BY src;
flows_by_dst = GROUP flows BY dst;
flows_by_comp_union = UNION flows_by_src, flows_by_dst;
flows_by_comp_gpd = GROUP flows_by_comp_union BY group;
flows_by_comp = FOREACH flows_by_comp_gpd {
	      		A = FOREACH flows_by_comp_union GENERATE flatten($1);
			GENERATE group AS comp, A AS flows;
	      };
flow_count = FOREACH flows_by_comp GENERATE comp,
	     	     		   	    COUNT(flows) AS nflow,
					    SUM(flows.bytes) AS nbyte;

--------------------------------
-- Method 2:
--
-- group and count by src and dst separately,
-- then sum the results.
-- Repeats code but keeps data output low.

flows_by_src = GROUP flows BY src;
flow_count_by_src = FOREACH flows_by_src GENERATE group AS cmp,
	     	     		   	    COUNT(flows) AS nflow,
					    SUM(flows.bytes) AS nbyte;
flows_by_dst = GROUP flows BY dst;
flow_count_by_dst = FOREACH flows_by_dst GENERATE group AS cmp,
	     	     		   	    COUNT(flows) AS nflow,
					    SUM(flows.bytes) AS nbyte;
flow_count_two_way = UNION flow_count_by_src, flow_count_by_dst;
flow_count_gpd = GROUP flow_count_two_way BY cmp;
flow_count = FOREACH flow_count_gpd GENERATE group AS cmp,
	     	     		   	    SUM(flow_count_two_way.nflow) AS nflow,
					    SUM(flow_count_two_way.nbyte) AS nbyte;

--------------------------------
-- Method 3:
--
-- Same as 2, but retain in and out information.

flows_by_src = GROUP flows BY src;
flow_count_by_src = FOREACH flows_by_src GENERATE group AS cmp,
	     	     		   	    COUNT(flows) AS outflow,
					    SUM(flows.bytes) AS outbyte;
flows_by_dst = GROUP flows BY dst;
flow_count_by_dst = FOREACH flows_by_dst GENERATE group AS cmp,
	     	     		   	    COUNT(flows) AS inflow,
					    SUM(flows.bytes) AS inbyte;
flow_count = JOIN flow_count_by_src BY cmp FULL OUTER, flow_count_by_dst BY cmp;

--------------------------------
-- write results:

STORE flow_count INTO 'comp_table' USING PigStorage(',');

--------------------------------
-- shell clean-up:

sh cat ./comp_table/part* > ./comp_table.txt
sh rm -r comp_table

---------------------------------------------------------------------------
-- Histograms of per-computer flow- and byte-counts:

flow_count = LOAD './comp_table.txt' USING PigStorage(',')
	     AS (src:chararray,
	     	 outflow:double,
		 outbyte:double,
		 dst:chararray,
	     	 inflow:double,
		 inbyte:double);

nflow_gpd = GROUP flow_count BY FLOOR(LOG(inflow + outflow)/LOG(2));
nflow_hist = FOREACH nflow_gpd GENERATE group AS lg_nflow, COUNT($1) AS freq;
\d nflow_hist

/*------
(1.0,25)
(2.0,61)
(3.0,84)
(4.0,110)
(5.0,118)
(6.0,146)
(7.0,137)
(8.0,218)
(9.0,309)
(10.0,395)
(11.0,675)
(12.0,1605)
(13.0,1750)
(14.0,1267)
(15.0,691)
(16.0,163)
(17.0,38)
(18.0,11)
(19.0,8)
(20.0,11)
(21.0,7)
(22.0,5)
(23.0,3)
(24.0,1)
(,3316)   -- computers with one of inflow/outflow equal to 0 
------*/

nbyte_gpd = GROUP flow_count BY FLOOR(LOG(inbyte + outbyte)/LOG(2));
nbyte_hist = FOREACH nbyte_gpd GENERATE group AS lg_nbyte, COUNT($1) AS freq;
\d nbyte_hist

/*------
(6.0,5)
(7.0,1)
(8.0,1)
(9.0,7)
(10.0,11)
(11.0,13)
(12.0,20)
(13.0,42)
(14.0,66)
(15.0,64)
(16.0,111)
(17.0,106)
(18.0,90)
(19.0,111)
(20.0,145)
(21.0,149)
(22.0,209)
(23.0,228)
(24.0,412)
(25.0,607)
(26.0,725)
(27.0,1447)
(28.0,1384)
(29.0,870)
(30.0,474)
(31.0,218)
(32.0,111)
(33.0,72)
(34.0,52)
(35.0,33)
(36.0,21)
(37.0,12)
(38.0,6)
(39.0,5)
(40.0,2)
(41.0,4)
(42.0,4)
(,3316)   -- computers with one of inbyte/outbyte equal to 0 
------*/

---------------------------------------------------------------------------
-- Count port frequencies using REGEX to group...

-- here's a simple way to do it:

server_pt = FOREACH flows GENERATE (dst_pt MATCHES 'N*'? '0': dst_pt);
server_pt_gpd = GROUP server_pt BY $0;
server_pt_count = FOREACH server_pt_gpd GENERATE group AS server_pt, COUNT($1);
\i server_pt_count

-- but let's restrict to TCP and pair client port with server port:

tcp_flows = FILTER flows BY proto==6;
tcp_pt_pair = FOREACH tcp_flows {
	    	    A = (REGEX_EXTRACT(src_pt, 'N', 0) is null? src_pt: '0');
		    B = (REGEX_EXTRACT(dst_pt, 'N', 0) is null? dst_pt: '0');
		    GENERATE A AS client_pt, B AS server_pt;
	  };
tcp_pt_gpd =  GROUP tcp_pt_pair BY (client_pt, server_pt);
tcp_pt_count = FOREACH tcp_pt_gpd GENERATE group AS pt_pair, COUNT($1) AS freq;

STORE tcp_pt_count INTO 'tcp_pt_count';
sh cat ./tcp_pt_count/part* > ./tcp_pt_count.txt
 
---------------------------------------------------------------------------
-- Graph of ICMP traffic

icmp_flows = FILTER flows BY proto==1;
icmp_edge_flows = GROUP icmp_flows BY (src, dst);
icmp_edges = FOREACH icmp_edge_flows
	     GENERATE group AS edge,
	     	      SUM(icmp_flows.duration) AS duration,
		      SUM(icmp_flows.bytes) AS bytes;

STORE icmp_edges INTO 'icmp_edges' USING PigStorage(',');
sh cat ./icmp_edges/part* | tr -d '()' > ./icmp_edges.txt

-- and load for follow-on calculations:

icmp_edges = LOAD './icmp_edges.txt' USING PigStorage(',')
	     AS (src:chararray,
	         dst:chararray,
	         duration:long,
		 bytes:long);

---------------------------------------------------------------------------
-- Count vertices and edges

-- (directed) edge count:
icmp_edges_gpd = GROUP icmp_edges ALL;
icmp_edge_count = FOREACH icmp_edges_gpd GENERATE COUNT($1);
\d icmp_edge_count

-- (43231)

-- vertex count:
icmp_from = FOREACH icmp_edges GENERATE src AS vertex;
icmp_to = FOREACH icmp_edges GENERATE dst AS vertex;
icmp_all = UNION icmp_from, icmp_to;
icmp_vertices = DISTINCT icmp_all;
icmp_vertices_gpd = GROUP icmp_vertices ALL;
icmp_vertex_count = FOREACH icmp_vertices_gpd GENERATE COUNT($1);

\d icmp_vertex_count

-- (8767)

/*--------------------------------
-- BUT note an apparent bug in 
-- case icmp_edges not stored/loaded,
-- but part of data flow from flows.txt;
-- lazy evaluation doesn't seem to work here...
-- see
-- <https://qnalist.com/questions/1905/union-causes-classcastexception>
-- and this is 4 years old!! :(
--------------------------------*/

---------------------------------------------------------------------------
-- Connected components


-- step 0: initialise virtual edges

virtual_edges_init = FOREACH icmp_edges GENERATE src, dst;
virtual_edges = FOREACH virtual_edges_init GENERATE src, dst;

-- step 1: generate index

virtual_edges_by_src = GROUP virtual_edges BY src;
index_rpt = FOREACH virtual_edges_by_src {
	    	A = FOREACH virtual_edges GENERATE flatten(TOBAG(src, dst));
      		B = MIN(A);
		GENERATE flatten(A) as src, B as dst;
      };
index = DISTINCT index_rpt;

-- step 2: count indices, decide on termination

indices_rpt = FOREACH index GENERATE dst;
indices = DISTINCT indices_rpt;
indices_gpd = GROUP indices ALL;
index_count = FOREACH indices_gpd GENERATE COUNT($1);

\d index_count

-- step 3: update virtual edges --> repeat steps 1,2

index_rev = FOREACH index GENERATE dst AS src, src AS dst;
virtual_edges_new = UNION virtual_edges_init, index;
virtual_edges_rpt = UNION virtual_edges_new, index_rev;
virtual_edges = DISTINCT virtual_edges_rpt;

-- counts after 5 successive iterations:

-- (2745)
-- (1308)
-- (15)
-- (5)
-- (3)

-- component sizes:

index_gpd = GROUP index BY dst;
comp_sizes = FOREACH index_gpd GENERATE group AS id, COUNT($1) AS size;
\d comp_sizes

-- (C10,8763)
-- (C15978,2)
-- (C20101,2)

---------------------------------------------------------------------------
-- Connected components - optimisation

-- Note that after 5 iterations above the process consists of 17 map-reduces:

\e index_count

-- Can we reduce this?

-- step 0: initialise virtual edges

virtual_edges_init = FOREACH icmp_edges GENERATE src, dst;
virtual_edges = FOREACH virtual_edges_init GENERATE src, dst;

-- step 1: generate index

virtual_edges_by_src = GROUP virtual_edges BY src;
index = FOREACH virtual_edges_by_src {
	    	A = FOREACH virtual_edges GENERATE flatten(TOBAG(src, dst));
		B = DISTINCT A;
      		C = MIN(B);
		GENERATE flatten(B) as src, C as dst;
      };

-- step 2: count indices, decide on termination

indices_rpt = FOREACH index GENERATE dst;
indices = DISTINCT indices_rpt;
indices_gpd = GROUP indices ALL;
index_count = FOREACH indices_gpd GENERATE COUNT($1);

-- step 3: update virtual edges --> repeat steps 1,2

index_rev = FOREACH index GENERATE dst AS src, src AS dst;
virtual_edges_new = UNION virtual_edges_init, index;
virtual_edges_rpt = UNION virtual_edges_new, index_rev;
virtual_edges = DISTINCT virtual_edges_rpt;

