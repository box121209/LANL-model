
---------------------------------------------------------------------------
-- Use the  PigStorage function to load the netflow file into the 'flows'
-- bag as an array of records.
-- Input: (time, duration, src, src_pt, dst, dst_pt, proto, pkts, bytes) 

flows = LOAD '/Users/wmoxbury/data/LANL/flows.txt' USING PigStorage(',') -- for real
-- flows = LOAD '/Users/wmoxbury/data/LANL/smallflows.txt' USING PigStorage(',') -- for dev
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

STORE flow_count INTO 'comp_table';

--------------------------------
-- shell clean-up:

sh cat ./comp_table/part* > ./comp_table.txt
sh rm -r comp_table

---------------------------------------------------------------------------
-- Histograms of per-computer flow- and byte-counts:

flow_count = LOAD './comp_table.txt' USING PigStorage('\t')
	     AS (comp:chararray, nflow:double, nbyte:double);

nflow_gpd = GROUP flow_count BY FLOOR(LOG(nflow)/LOG(2));
nflow_hist = FOREACH nflow_gpd GENERATE group AS lg_nflow, COUNT($1) AS freq;
\d nflow_hist

nbyte_gpd = GROUP flow_count BY FLOOR(LOG(nbyte)/LOG(2));
nbyte_hist = FOREACH nbyte_gpd GENERATE group AS lg_nbyte, COUNT($1) AS freq;
\d nbyte_hist

-- 
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

STORE icmp_edges INTO 'icmp_edges';
sh cat ./icmp_edges/part* > ./icmp_edges.txt
icmp_edges = LOAD './icmp_edges.txt' USING PigStorage('\t')
	     AS ();

---------------------------------------------------------------------------
-- Count vertices and edges

-- (directed) edge count:
icmp_edges_gpd = GROUP icmp_edges ALL;
icmp_edge_count = FOREACH icmp_edges_gpd GENERATE COUNT($1);
\d icmp_edge_count

-- vertex count:
icmp_from_rpt = FOREACH icmp_edges GENERATE edge.$0 AS vertex;
icmp_from = DISTINCT icmp_from_rpt;

icmp_to_rpt = FOREACH icmp_edges GENERATE edge.$1 AS vertex;
icmp_to = DISTINCT icmp_to_rpt;

/*--------------------------------
-- fix for an apparent bug;
-- lazy evaluation doesn't seem to work...
-- see
-- https://qnalist.com/questions/1905/union-causes-classcastexception
-- BUT this is 4 years old!! :(

STORE icmp_from INTO 'icmp_from';
STORE icmp_to INTO 'icmp_to';

-- recover:
sh cat ./icmp_from/part* > ./icmp_from.txt
sh rm -r icmp_from
icmp_from = LOAD './icmp_from.txt' USING PigStorage('\t')
	     AS (vertex:chararray);

sh cat ./icmp_to/part* > ./icmp_to.txt
sh rm -r icmp_to
icmp_to = LOAD './icmp_to.txt' USING PigStorage('\t')
	     AS (vertex:chararray);
--------------------------------*/

icmp_all = UNION icmp_from, icmp_to;
icmp_vertices = DISTINCT icmp_all;
icmp_vertices_gpd = GROUP icmp_vertices ALL;
icmp_vertex_count = FOREACH icmp_vertices_gpd GENERATE COUNT($1);
\d icmp_vertex_count


---------------------------------------------------------------------------