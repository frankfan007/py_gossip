###################################
# Standard node evolution chart:
#
# 	node_1	node_2	node_3
#	20	40	30
#	25	40	25
#	32.5	32.5	25
#	30	30	30
#
# Cycle number is assumedly in order
# Default data type is data.frame
####################################


# To read CSV file
# Can adapt to different form of data
# Input: CSV file
# Output: standard node evolution chart
usr_read <- function(resource="fd_with_id.csv", nodeID=TRUE, cycleNum=FALSE, transpose=TRUE) {
	temp <- data.frame()
	if (transpose) {
		if (nodeID && cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=1)
		} else if (nodeID) {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=1)
		} else if (cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=NULL)
			rownames(temp) <- gen_name(num=nrow(temp))
		} else {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=NULL)
			rownames(temp) <- gen_name(num=nrow(temp))
		}
		temp <- as.data.frame(t(temp))
	} else {
		if (nodeID && cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=1)
		} else if (nodeID) {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=1)
		} else if (cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=NULL)
		} else {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=NULL)
		}
	}
	return(temp)
}

# Generate name using format: name_#
# Output: Vector of char
gen_name <- function(preffix="nodeID", num=7) {
	temp <- vector()
	for (i in 1:num) {
		temp <- c(temp, paste(preffix, i, sep="_"))
	}
	return(temp)
}


#calculate variance for every row
# Input: data.frame
# Output: a vector of the variance of each row
var_vector <- function(data_frame, print=FALSE) {
	m <- as.matrix(data_frame)
	vec <- vector()
	for (i in 1:nrow(data_frame)) {
		vec[i] <- var(m[i,])
		#cat(vec,"\n")
	}
	if (print==TRUE) {
		vec
	}
	return(vec)
}

# Take a list of data.frame, could be the run result ofdifferent topology of network
# Input: list(data.frame)
# Output: data.frame() of variance in each cycle and topology
var_frame <- function(frame_list, name_list) {
	lst <- list()
	if(missing(name_list)) {
		name_list <- c(as.character(1:length(frame_list)))
	}
	for (i in 1:length(frame_list)) {
		lst[[i]] <- c(var_vector(frame_list[[i]]))
	}
	fra <- do.call(cbind.data.frame, lst)
	colnames(fra) <- name_list
	return(fra)
}

# append cycle column and melt the data.frame for plotting
# Input: data.frame
# Output: melted data.frame
cycle_and_melt <- function(data_frame) {
	temp <- data_frame
	temp$cycle=1:nrow(temp)
	melted<-melt(temp, id="cycle")
	return(melted)
}

# input: standard node evolution chart
# output: convergence graph
plot_var <- function(data_frame) {
	melted <- cycle_and_melt(data_frame)
	p <- ggplot(melted, aes(x=cycle, y=value, colour=variable))
	p <- p + geom_line()
	return(p)
}

# input: standard node evolution chart, with nodeID
# output: scatter plot
# modify geom_point() to adjust appearance
plot_convergence <- function(data_frame) {
	melted <- cycle_and_melt(data_frame)
	p <- ggplot(melted, aes(x=cycle, y=value))
	p <- p + geom_point(alpha=I(0.3), size=2)
	p <- p + xlab("# of Cycle") + ylab("stateValue")
	
	return(p)
}

# strip duplicated epochs
strip_data <- function(data) {
	return(data[-which(duplicated(data$epoch) %in% TRUE),])
}

# read Node*.csv files
read_node_csv <- function() {
	ret <- list()
	file_list <- list.files(".",pattern="Node[0-3][0-9].csv")
	min <- 200
	for (node in file_list) {
		# print(paste("reading ", node))
		node_data <- read.csv(node, sep=",", header=TRUE, row.names=NULL)
		node_data.stripped <- strip_data(node_data)
		node_data.name <- substr(node, start=1, stop=6)
		node_data.list <- as.list(node_data.stripped)
		ret <- c(ret, list(node_data.list$state))
		if (length(node_data.list$epoch) < min) {
			min <- length(node_data.list$epoch)
		}
	}
	ret.mat <- do.call(cbind, ret)
	ret.df <- as.data.frame(ret.mat)
	ret.df <- ret.df[-(min:nrow(ret.df)),]
	# ret.df <- as.data.frame(ret)
	# ret.df <- ret.df[-(min:nrow(ret.df)),]
	return(ret.df)
}

# read Node*.csv files as raw
read_node_raw <- function(dir=".") {
	ret <- data.frame()
	file_list <- list.files(dir,pattern="Node[0-3][0-9].csv")
	for (node in file_list) {
		# print(paste("reading ", node))
		node_data <- read.csv(node, sep=",", header=TRUE, row.names=NULL)
		ret <- rbind(ret, node_data)
	}
	return(ret)
}

# plot raw data state-epoch
plot_raw <- function(data_frame, title="foo") {
	p <- ggplot(data_frame, aes(x=epoch, y=state))
	p <- p + geom_point(alpha=I(0.3), size=2)
	p <- p + xlab("Epoch") + ylab("State")
	p <- p + ggtitle(title)
	
	return(p)
}

# strip duplicated epochs
cal_var <- function(raw, title="bar") {
	raw.var <- data.frame()
	max <- max(raw$epoch)
	for (i in 1:max) {
		tmp <- var(raw$state[which(raw$epoch == i)])
		raw.var <- rbind(raw.var, c(i, tmp))
	}
	colnames(raw.var)[1] <- "epoch"
	colnames(raw.var)[2] <- "title"
	return(raw.var)
}
