
# Problem Set 6
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

#' @title Title deviation bootstrap 
#' @param i sampling order 
#' @return RF deviation for random player 
boot <- function(i) {
  for(k in team_list) {
    team_strat <- team_RF$teamid[strat] # team selection
    mean_strat <- team_RF$RF[team_RF$teamid==k] # the RF mean of the selected team
    sample_num <- sample(1:sum(fielding$teamid == team_strat), 1) # random number for player
    player_num <- fielding$RF[fielding$teamid == team_strat][sample_num] # player selection
    deviation_num <- (player_num-mean_strat)^2 #deviation 
    
  }
    return(deviation_num)
}

x <- c("AKL" = 0.335, "LLL" = 0.222, "AKL"=0.444, "LLL"=0.111)
nl <- split(x, names(x))
sapply(nl, mean)
nm <- tapply(x, names(x), mean)
nm

nsim <- 1000
sd_RF_a <- sqrt(sum(sapply(1:nsim, boot))/(nsim-1)) # sample standard deviation
sum(fielding$teamid == "ATL")

#a-2. Using parallel processing

library(parallel)
library(parallelly)

# This code comes from STATS 506 Class Note
try(stopCluster(cl), silent = TRUE); cl <- makeCluster(8)
clusterExport(cl, varlist = c("fielding", "team_RF", "boot"))
sd_RF_b <- sqrt(sum(parSapply(cl, 1:nsim, boot))/(nsim-1))
stopCluster(cl)

#a-3. Using future
library(future)
plan(multisession)
sd_f <- future(sqrt(sum(sapply(1:nsim, boot))/(nsim-1)))
sd_RF_c <- suppressWarnings(value(sd_f))
sd_RF_c


#b. a table showing the estimated RF and associated standard errors
results <- cbind(result1, result2, result3)
names(results) <- c("Sequence", "Parallel", "Future")
library(knitr)
kable(results, align = "cc")

#c. The performance difference between the versions
system.time(results <- parLapply(cl, 1:100, boot))
system.time(results <- lapply(1:100, boot))
system.time({
  bcfuture <- future(clean_batting(batting))
  fc <- clean_fielding(fielding)
  bc <- value(bcfuture)
})

#(discussion) 