# Build feature vectors on bboxes (at least 100 flows seen) and 
# dimensional reduction by t-SNE.

# (1) Feature vectors are 12 concatenated weighted probability distributions:
#     probability over protocols for
#     - outgoing flow
#     - outgoing byte
#     - outgoing flow-second
#     probability over destination server ports for
#     - outgoing flow
#     - outgoing byte
#     - outgoing flow-second
#     probability over protocols for
#     - incoming flow
#     - incoming byte
#     - incoming flow-second
#     probability over destination server ports for
#     - incoming flow
#     - incoming byte
#     - incoming flow-second

# (2) Weighting is designed to amplify probabilities away from dominant
#     states e.g. UDP, port 80. It should increase the entropy.

# (3) Features are all then in the unit cube, with comparable variances.
#     Distance is Euclidean.

setwd("~/Blog/aws/_INPROGRESS_201605lanl-rnn/LANL-model")
source("R/init.R")

threshold <- 100
dat.proto <- protofeat[protofeat$nflow >= threshold,]
dat.port <- portfeat[portfeat$nflow >= threshold,]
nbyte <- nbyte[nflow[,2] >= threshold,]
bbox <- nbyte[,1]

##############################################################################
# protocol probabilities:
out.proto.fprob <- aggregate(dat.proto$outflows/dat.proto$nflow, list(dat.proto$box), c)$x
out.proto.bprob <- aggregate(dat.proto$outflows*dat.proto$outbytesav/dat.proto$nbyte, list(dat.proto$box), c)$x
out.proto.sprob <- aggregate(dat.proto$outflows*dat.proto$outdurationav/dat.proto$nsec, list(dat.proto$box), c)$x
in.proto.fprob <- aggregate(dat.proto$inflows/dat.proto$nflow, list(dat.proto$box), c)$x
in.proto.bprob <- aggregate(dat.proto$inflows*dat.proto$inbytesav/dat.proto$nbyte, list(dat.proto$box), c)$x
in.proto.sprob <- aggregate(dat.proto$inflows*dat.proto$indurationav/dat.proto$nsec, list(dat.proto$box), c)$x

# port probabilities:
out.port.fprob <- aggregate(dat.port$outflows/dat.port$nflow, list(dat.port$box), c)$x
out.port.bprob <- aggregate(dat.port$outflows*dat.port$outbytesav/dat.port$nbyte, list(dat.port$box), c)$x
out.port.sprob <- aggregate(dat.port$outflows*dat.port$outdurationav/dat.port$nsec, list(dat.port$box), c)$x
in.port.fprob <- aggregate(dat.port$inflows/dat.port$nflow, list(dat.port$box), c)$x
in.port.bprob <- aggregate(dat.port$inflows*dat.port$inbytesav/dat.port$nbyte, list(dat.port$box), c)$x
in.port.sprob <- aggregate(dat.port$inflows*dat.port$indurationav/dat.port$nsec, list(dat.port$box), c)$x


##############################################################################
# FULL FEATURE SET

raw.feat <- do.call(cbind, list(
  out.proto.fprob,
  out.proto.bprob,
  out.proto.sprob,
  in.proto.fprob,
  in.proto.bprob,
  in.proto.sprob,
  out.port.fprob,
  out.port.bprob,
  out.port.sprob,
  in.port.fprob,
  in.port.bprob,
  in.port.sprob
))

# replace NaN's with 0:
for(i in 1:nrow(raw.feat))
  for(j in 1:ncol(raw.feat))
    if(is.nan(raw.feat[i,j]))
      raw.feat[i,j] <- 0.0


##############################################################################
# t-SNE plot 

library(Rtsne)

# project to dimension 4 -- more space to move around, but not not too
# expensive in t-SNE training. Also avoids curse of dimension?

tsne.output <- Rtsne(raw.feat, 
                     theta=0.4,
                     max_iter=5000,
                     dims=4,
                     verbose=TRUE, 
                     check_duplicates=FALSE
                     )

tsne.proj <- data.frame(tsne.output$Y)
names(tsne.proj) <- sapply(1:4, function(i) sprintf("x%d", i))

# eyeball:
pairs(tsne.proj, cex=0.1)

##############################################################################
# save to disk:

filename <- sprintf("data/box_profiles_20160524_%d.Rds", threshold)
save(bbox,
     protos,
     ports,
     nbyte,
     out.proto.fprob,
     out.proto.bprob,
     out.proto.sprob,
     in.proto.fprob,
     in.proto.bprob,
     in.proto.sprob,
     out.port.fprob,
     out.port.bprob,
     out.port.sprob,
     in.port.fprob,
     in.port.bprob,
     in.port.sprob,
     tsne.proj,
     file=filename)

coords <- data.frame(bbox, tsne.proj)
write.csv(coords, file="data/box_coords.csv", quote=FALSE, row.names=FALSE)
# then delete header row manually (col.names=FALSE doesn't seem to work)

##############################################################################
# reload:

load("data/box_profiles_20160524_100.Rds")




