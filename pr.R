
# Problem Set 6
# a. a Stratified Bootstrap by Team
# a-1. Without any parallel processing 

library(DBI)

lahman <- dbConnect(RSQLite::SQLite(), "lahman_1871-2022.sqlite")
fielding <- dbGetQuery(lahman, "SELECT * FROM FIELDING")
names(fielding) <- tolower(names(fielding))

po <- fielding$po # Put Outs
a <- fielding$a # Assist
innouts <- fielding$innouts # Inn Outs
fielding$RF <- 3*(po+a)/innouts # Range Factor
fielding <- fielding[!is.na(fielding$RF) & fielding$RF!=Inf, ] # Remove rows where RF is NA or Inf ("Inn Out" is NA or Zero)

# average RF for each team 
team_RF <- aggregate(RF ~ teamid, FUN = "mean", data = fielding, na.rm = TRUE)
team_list <- team_RF$teamid

#' @title deviation bootstrap 
#' @param i sampling order 
#' @return RF deviation for random player 
boot <- function(i) {
  dev_list <- list()
  for(k in team_list) {
    mean_strat <- team_RF$RF[team_RF$teamid==k] # the RF mean of the team k
    sample_num <- sample(1:sum(fielding$teamid == k), 1) # random number for player
    player_num <- fielding$RF[fielding$teamid == k][sample_num] # player selection
    deviation_num <- (player_num-mean_strat)^2 # deviation from mean for each player
    dev_list[k] <- deviation_num
  }
  return(dev_list)
}

#' @title Calculate standard deviation of bootstrap samples 
#' @param data the number of bootstrap samples
#' @return sample standard deviation
boot_sd <- function(data) {
  data <- as.data.frame(do.call(rbind, data)) # arrange data by team 
  data <- sapply(data, as.numeric) # transform data from list to numeric
  dt_sum <- apply(data, 2, sum) # calculate the sum of deviations 
  dt_sd <- sqrt(dt_sum/(nsim-1)) # calculate standard deviation
  return(dt_sd)
}

nsim <- 1000
boot_sample_default <- lapply(1:nsim, boot) # make bootstrap samples
sd_RF_default <- boot_sd(boot_sample_default) # calculate the sample standard deviation


#a-2. Using parallel processing

library(parallel)
library(parallelly)

# This code comes from STATS 506 Class Note
try(stopCluster(cl), silent = TRUE); cl <- makeCluster(8)
clusterExport(cl, varlist = c("fielding", "team_RF", "team_list", "boot"))
boot_sample_parallel <- parLapply(cl, 1:nsim, boot) # make bootstrap samples
sd_RF_parallel <- boot_sd(boot_sample_parallel) # calculate the sample standard deviation
stopCluster(cl)

#a-3. Using future
library(future)
plan(multisession)
boot_sample_future <- future(lapply(1:nsim, boot)) # make bootstrap samples
boot_sample_future <- suppressWarnings(value(boot_sample_future)) # generate samples
sd_RF_future <- boot_sd(boot_sample_future) # calculate the sample standard deviation
plan(sequential)

#b. a table showing the estimated RF and associated standard errors

sorted_RF <- team_RF[order(team_RF$RF, decreasing = TRUE), ] # sort RF
top10_RF <- sorted_RF[1:10, ] # top 10 RF teams 
rownames(top10_RF) <- 1:10 # change row names 

for(i in top10_RF$teamid) {
  # make three columns for the standard deviation from the three approaches
  top10_RF$default_sd[top10_RF$teamid == i] <- sd_RF_default[i] 
  top10_RF$parallel_sd[top10_RF$teamid == i] <- sd_RF_parallel[i] 
  top10_RF$future_sd[top10_RF$teamid == i] <- sd_RF_future[i]
}
top10_RF # top10 table 


#c. The performance difference between the versions

library(microbenchmark)

default_time <- system.time({
   lapply(1:nsim, boot)
})

parallel_time <- system.time({
  try(stopCluster(cl), silent = TRUE); cl <- makeCluster(8)
  clusterExport(cl, varlist = c("fielding", "team_RF", "team_list", "boot"))
  parLapply(cl, 1:nsim, boot)
  stopCluster(cl)
})

future_time <-  system.time({
  plan(multisession)
  future_sample <- future(lapply(1:nsim, boot)) # make bootstrap samples
  suppressWarnings(value(boot_sample_future))
  plan(sequential)
})
  

rbind(default_time, parallel_time, future_time)

#(discussion) 