# path to folder that holds multiple .csv files
folder <- "C:/Users/matthew.hartwick/Downloads/Paid Search Files/"

# create list of all .csv files in folder
file_list <- list.files(path=folder, pattern="*.csv")

# read in each .csv file in file_list and rbind them into a data frame called data
data <-
  do.call("rbind",
          lapply(file_list,
                 function(x)
                   read.csv(paste(folder, x, sep=''), numerals = "no.loss",
                             stringsAsFactors=TRUE,as.is=TRUE,
                            col.names=c("ad_id","advertiser_id","campaign_id","paid_search_ad_id","paid_search_legacy_keyword_id","paid_search_keyword_id","paid_search_campaign","paid_search_ad_group","paid_search_bid_strategy","paid_search_landing_page_url","paid_search_keyword","paid_search_match_type"),quote = "")))

# convert paid_search_ad_id from char to numeric
data$paid_search_ad_id2 <- as.numeric(data$paid_search_ad_id)

# write data frame to csv file
write.csv(data, file = "paid_search_match_20171001-20171031.csv", row.names=FALSE,quote=FALSE)

# create data frame of unique records
# unexpected results; not using
# data2 <- unique(data)

# write.csv(data2, file = "paid_search_match_20170526-20170727_slim.csv", row.names=FALSE,quote=FALSE)