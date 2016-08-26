

ports <- read.csv(pipe("cat tcp_pt_count.txt | tr -d '()' | tr '\t' ','"))

library(igraph)

from <- as.character(ports[,1])
to <- as.character(ports[,2])
wt <- ports[,3]
df <- data.frame(from, to, wt)

g <- graph.edgelist(as.matrix(df[,1:2]))

V(g)$size <- log(1 + degree(g))
V(g)$color <- 'white'
V(g)$label.cex <- 0.05*V(g)$size
E(g)$arrow.mode <- '-'
lout <- layout.fruchterman.reingold(g)

plot(g, layout=lout)

