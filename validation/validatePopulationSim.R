#######################################################################################################
# Validation script for PopulationSim
#
# binny.paul@rsginc.com, Jan 2018
# 
# This script uses summary outputs from a PopulationSim run to generate validation summaries and plots
#
# User needs to specify the following inputs and settings:-
#
#         PopulationSim working directory [the directory containing data, configs and output folders]
#         Validation directory [the directory where you would want all your validation summaries]
#         scenario name
#         region name
#         List of geographies - from highest to lower geography [e.g., REGION > PUMA > TAZ > MAZ]
#                               First Meta, second Seed and then all Sub-Seed from the highest to the lowest
#         Plot geographies - any geography other than Seed geography. Plots will be generated only for the 
#                            geographies listed here for the controls for that geography
#         Geographic crosswalk file name [assumed to be inside the data folder in PopulationSim working dir]
#         Column Map CSV - CSV file to specify the controls for which the summaries should be generated. FOllowing 
#                          columns need to be specified:
#             NAME       : Name of the control to be used for labels
#             GEOGRAPHY  : Geography at which the control was specified
#             CONTROL    : Control value column name in the 
#                          summary_GEOGRAPHY file [summary_PUMA file for Meta controls] 
#             SUMMARY    : Estimate/Result value column name in the 
#                          summary_GEOGRAPHY file [summary_PUMA file for Meta controls]
#
#         Input seed household sample file
#         List of column names in the input seed household sample (seed_col) for following:
#             Seed geography name
#             Unique household ID
#             Initial household weight
#         
#         Column name of the unique HH ID (expanded_hhid_col) in the 
#         expanded household id file (expanded_household_ids.csv). 
#         
#         This is the column name assigned to the unique household ID in the initial seed sample in the 
#         PopulationSim YAML settings file.
#         
#         The user should also configure the PopulationSim to produce following summary files in the output folder:
#                 expanded_household_ids.csv
#                 summary_GEOGRAPHY.csv (for all sub-seed geographies, e.g., summary_TRACT.csv)
#                 summary_LOWEST_PUMA.csv (PUMA level summaries for the lowest gepgraphy, e.g., summary_TAZ_PUMA.csv)
#
#
# List of outputs:-
# 
# CSV summary Statistics file - It has the following statistics:
#         control_name - Name of the control
#         geography - Geography at which the control is specified
#         observed - Regional total specified
#         predicted - Regional total synthesized
#         difference - predicted - observed
#         pct_difference - Percentage difference at a regional level
#         N - Number of geographies (MAZ/TAZ/META) with non-zero control
#         RMSE - Percentage root mean square error for the control at the specified geography
#         sdev - Standard deviation of precentage difference
#
# Plots (JPEGs) - convergence plots:
#
#         Plot showing mean percentage difference and STDEV for each control
#         Plot showing frequency distribution of differences b/w target and estimate for each control
#         Plot showing expansion factor distribution
#
##############################################################################################################
# args                <- commandArgs(trailingOnly = TRUE)
# Parameters_File     <- args[1]
# Parameters_File     <- "E:\\Projects\\Clients\\FresnoCOG\\Tasks\\PopulationSim\\Data\\parameters.csv"
### User Inputs [Read command line arguments]
# parameters <- read.csv(Parameters_File, header = TRUE)
# WORKING_DIR <- trimws(paste(parameters$Value[parameters$Key=="WORKING_DIR"]))	
# popsim_dir       <- trimws(paste(parameters$Value[parameters$Key=="POPSIMDIR"]))	
# validation_dir   <- trimws(paste(parameters$Value[parameters$Key=="VALID_DIR"]))	

popsim_dir       <- file.path(getwd(), "populationsim")
validation_dir   <- file.path(popsim_dir, "validation")

geography_list   <- c("REGION", "STATE", "PUMA", "TRACT", "BG")
plot_geographies <- c("REGION", "STATE", "PUMA", "TRACT", "BG")
scenario = 'AK-WY'

# geogXWalk       <- read.csv(paste(popsim_dir, "data/geo_crosswalks.csv", sep = "/"))
# column_map       <- read.csv(paste(validation_dir, "column_mapPopSim_Dubai_em.csv", sep = "/"))
column_map       <- read.csv(file.path(popsim_dir, "configs/controls.csv"))

seed_households <- read.csv(file.path(popsim_dir, "data", scenario, "seed_households.csv"))
seed_col        <- c("household_id", "HH_WEIGHT")

expanded_hhid      <- read.csv(file.path(popsim_dir, "output_mp", scenario, "final_expanded_household_ids.csv"))
expanded_hhid_col  <- c("household_id")

#   This is currently configured for 2 sub-seed geography
#   User should add more read lines when more geographies is involved
#   The nummber at the end of summary file name indicates the geographic index
#   Example, summary3 is the name for summary_TRACT which is the 3rd geography in the geography list
# summary1           <- read.csv(file.path(popsim_dir, "output_mp", scenario, "final_summary_TRACT_STATE.csv"))
# summary2           <- read.csv(paste(popsim_dir, "output/em/summary_MAZ_PUMA.csv", sep = "/"))
summary1           <- read.csv(file.path(popsim_dir, "output_mp", scenario, "final_summary_BG_REGION.csv"))
summary2           <- read.csv(file.path(popsim_dir, "output_mp", scenario, "final_summary_BG_STATE.csv"))
summary3           <- read.csv(file.path(popsim_dir, "output_mp", scenario, "final_summary_BG_PUMA.csv"))
summary4           <- read.csv(file.path(popsim_dir, "output_mp", scenario, "final_summary_TRACT.csv"))
summary5           <- read.csv(file.path(popsim_dir, "output_mp", scenario, "final_summary_BG.csv"))
# summary3           <- read.csv(paste(popsim_dir, "output/em/final_summary_Community.csv", sep = "/"))
# summary4           <- read.csv(paste(popsim_dir, "output/em/final_summary_TAZ.csv", sep = "/"))
# print(summary1)
# print(summary2)
# summary2$meta_geog <- geogXWalk$REGION[match(summary2$id, geogXWalk$PUMA)]
# summary2$meta_geog <- geogXWalk$COUNTY[match(summary2$id, geogXWalk$PUMA)]
#summary1           <- summary2
#summary1$geography <- geography_list[1]
#summary1$id        <- summary1$meta_geog
#summary1$id        <- summary1$REGION

### Install all required R packages to the user specified directory
#   [Make sure the R library directory has write permissions for the user]
### Load libraries
r_pkgs <- c("RODBC", "dplyr", "ggplot2", "tidyr", "scales", "hydroGOF", "stringr", "data.table")

for (pkg in r_pkgs) {
  if (!pkg %in% installed.packages()) {
    install.packages(pkg, repos = 'http://cran.us.r-project.org')
  } else {
    print(paste(pkg, 'installed.'))
  }    
}

lib_sink <- suppressWarnings(suppressMessages(lapply(r_pkgs, library, character.only = TRUE)))

calc_prmse = function(control, synth) {
  prmse <- (((sum((control - synth)^2) / (sum(control > 0) - 1))^0.5) / sum(control)) * sum(synth > 0) * 100
  return(prmse)
}

my_rmse <- function(final_exp, avg_exp, n) {
  expected <- rep(avg_exp, n)
  actual <- final_exp
  return(rmse(actual, expected, na.rm = TRUE))
}

### Function to process each control
proc_control <- function(map) {

  geography = map['geography']
  control_id = paste0(map['target'], "_control")
  summary_id = paste0(map['target'], "_result")
  control_name = map['control_field']
  
  # print("Processing geography:")
  # print(geography)
  #Fetching data
  sub_summary = fread(file.path(popsim_dir, "output_mp", scenario, str_glue("final_summary_{geography}.csv")))

  geo_index <- which(geography_list == geography)
  # print("which comes at index")
  # print(geo_index)
  # ev1 <- paste("sub_summary <- summary", geo_index, sep = "")
  # eval(parse(text = ev1))
  # print("This is the names(sub_summary)")
  # print(names(sub_summary))
  controls = sub_summary[, c('id', control_id), with = FALSE]
  synthesized = sub_summary[, c('id', summary_id), with = FALSE]

  # print("Parameters")
  # print("THis is the control_id")
  # print(control_id)# control_name, control_id, summary_id)
  # print(controls)
  # print("This is the control head")
  # print(head(controls))
  colnames(controls) <- c("GEOGRAPHY", "CONTROL")
  colnames(synthesized) <- c("GEOGRAPHY", "SYNTHESIZED")
  
  # Meta controls are grouped by PUMAs, aggregation is required
  if (geo_index == 1) {
    # aggregate control to meta geography
    controls <- controls %>%
      group_by(GEOGRAPHY) %>%
      summarise(CONTROL = sum(CONTROL)) %>%
      ungroup()
    
    # aggregate synthesized to meta geography
    synthesized <- synthesized %>%
      group_by(GEOGRAPHY) %>%
      summarise(SYNTHESIZED = sum(SYNTHESIZED)) %>%
      ungroup()
  }
  
  #Fetch and process each control for getting convergance statistics
  compare_data <- left_join(controls, synthesized, by = "GEOGRAPHY") %>%
    mutate(CONTROL = as.numeric(CONTROL)) %>%
    mutate(SYNTHESIZED = ifelse(is.na(SYNTHESIZED), 0, SYNTHESIZED)) %>%
    mutate(DIFFERENCE = SYNTHESIZED - CONTROL) %>%
    mutate(pcDIFFERENCE = ifelse(CONTROL > 0, (DIFFERENCE / CONTROL) * 100, NA))
  
  #Calculate statistics
  observed <- sum(compare_data$CONTROL)
  predicted <- sum(compare_data$SYNTHESIZED)
  difference <- predicted - observed
  pct_difference <- (difference / observed) * 100
  N <- sum(compare_data$CONTROL > 0) # nolint: object_name_linter.
  prmse <- calc_prmse(compare_data$CONTROL, compare_data$SYNTHESIZED)
  mean_pct_diff <- mean(compare_data$pcDIFFERENCE, na.rm = TRUE)
  sdev <- sd(compare_data$pcDIFFERENCE, na.rm = TRUE)
  stats <- data.frame(
    control_name, geography, observed, predicted, 
    difference, pct_difference, N, prmse, mean_pct_diff, sdev
  )
  # print(control_name)
  # print(stats)
  #Preparing data for difference frequency plot
  freq_plot_data <- compare_data %>%
    filter(CONTROL > 0) %>%
    group_by(DIFFERENCE) %>%
    summarise(FREQUENCY = n())
  
  if (geography %in% plot_geographies) {
    #computing plotting parameters
    xaxis_limit <- max(abs(freq_plot_data$DIFFERENCE)) + 10
    plot_title <- paste("Frequency Plot: Syn - Control totals for", control_name, sep = " ")
    
    #Frequency Plot
    p1 <- ggplot(freq_plot_data, aes(x = DIFFERENCE, y = FREQUENCY)) +
      geom_point(colour = "coral") +
      coord_cartesian(xlim = c(-xaxis_limit, xaxis_limit)) +
      geom_vline(xintercept = c(0), colour = "steelblue") +
      labs(title = plot_title)

    fname = file.path(validation_dir, "plots", paste(control_id, ".png", sep = ""))
    ggsave(fname, plot = p1, width = 9, height = 6)
  }
  
  cat("\n Processed Control: ", control_name) 
  return(stats)

}

#Create plot directory
dir.create(file.path(validation_dir, 'plots'), showWarnings = FALSE)

### Computing convergance statistics and write out results
stats <- apply(column_map, 1, proc_control)
print("\n done processing controls")
stats = rbindlist(stats)
fname = file.path(validation_dir, paste(scenario, "Emirati PopulationSim stats.csv", sep = ""))
write.csv(stats, file = fname, row.names = FALSE)

#for fresno only - set PRMSE and sdev to 0 as there is only one region
stats[stats$geography == 'REGION', ]$prmse <- 0 
stats[stats$geography == 'REGION', ]$sdev <- 0

### Convergance plot
p2 <- ggplot(stats, aes(x = control_name, y = mean_pct_diff)) +
  geom_point(shape = 15, colour = "steelblue", size = 2) +
  geom_errorbar(data = stats, aes(ymin = -sdev, ymax = sdev), width = 0.2, colour = "steelblue") +
  scale_x_discrete(limits = rev(levels(stats$control_name))) + 
  geom_hline(yintercept = c(0)) +
  labs(x = NULL, y = "Percentage Difference [+/- SDEV]", 
    title = "Region PopulationSim Controls Validation"
  ) +
  coord_flip(ylim = c(-50, 50)) +
  theme_bw() +
  theme(plot.title = element_text(size = 12, lineheight = .9, face = "bold", vjust = 1))

fname = file.path(validation_dir, "plots", paste(scenario, "Emirati PopulationSim Convergance-sdev.jpeg", sep = ""))
ggsave(file = fname, width = 8, height = 10)

### Convergance plot
p3 <- ggplot(stats, aes(x = control_name, y = mean_pct_diff)) +
  geom_point(shape = 15, colour = "steelblue", size = 2) +
  geom_errorbar(data = stats, aes(ymin = -prmse, ymax = prmse), width = 0.2, colour = "steelblue") +
  scale_x_discrete(limits = rev(levels(stats$control_name))) + 
  geom_hline(yintercept = c(0)) +
  labs(x = NULL, y = "Percentage Difference [+/- PRMSE]",
   title = "Region PopulationSim Controls Validation"
   ) +
  coord_flip(ylim = c(-50, 50)) +
  theme_bw() +
  theme(plot.title = element_text(size = 12, lineheight = .9, face = "bold", vjust = 1))

fname = file.path(validation_dir, "plots", paste(scenario, "Emirati PopulationSim Convergance-PRMSE.jpeg", sep = ""))
ggsave(file = fname, plot = p3, width = 8, height = 10)


### Uniformity Analysis
summary_hhid <- expanded_hhid %>% 
  mutate(FINALWEIGHT = 1) %>%
  select(FINALWEIGHT, expanded_hhid_col) %>%
  group_by(household_id) %>%
  summarise(FINALWEIGHT = sum(FINALWEIGHT))
  
uniformity <- seed_households[seed_households$HH_WEIGHT > 0, seed_col] %>%
  # left_join(summary_hhid, by = c("hhnum" = "household_id")) %>%
  left_join(summary_hhid, by = c("household_id" = "household_id")) %>%
  mutate(FINALWEIGHT = ifelse(is.na(FINALWEIGHT), 0, FINALWEIGHT)) %>%
  mutate(EXPANSIONFACTOR = FINALWEIGHT / HH_WEIGHT) %>%
  mutate(EFBIN = cut(EXPANSIONFACTOR, seq(0, max(EXPANSIONFACTOR) + 0.5, 0.5), right = FALSE, include.lowest = FALSE))

u_analysis_puma <- group_by(uniformity, PUMA, EFBIN)

ef_plot_data <- summarise(u_analysis_puma, PC = n()) %>%
  mutate(PC = PC / sum(PC))

p4 <- ggplot(ef_plot_data, aes(x = EFBIN, y = PC))  + 
  geom_bar(colour = "black", fill = "#DD8888", width = .7, stat = "identity") + 
  guides(fill = FALSE) +
  xlab("RANGE OF EXPANSION FACTOR") + ylab("PERCENTAGE") +
  ggtitle("EXPANSION FACTOR DISTRIBUTION BY PUMA") + 
  facet_wrap(~PUMA, ncol = 6) + 
  theme_bw() +
  theme(axis.title.x = element_text(face = "bold"),
        axis.title.y = element_text(face = "bold"),
        axis.text.x  = element_text(angle = 90, size = 5),
        axis.text.y  = element_text(size = 5))  +
  scale_y_continuous(labels = percent_format())

fname = file.path(validation_dir, "plots/EF-Distribution.jpeg")
ggsave(p4, file = fname, width = 15, height = 10)

u_analysis_puma <- group_by(uniformity, PUMA)

u_analysis_puma <- summarize(u_analysis_puma
                           , W = sum(HH_WEIGHT)
                           , Z = sum(FINALWEIGHT)
                           , N = n()
                           , EXP = Z / W
                           , EXP_MIN = min(EXPANSIONFACTOR)
                           , EXP_MAX = max(EXPANSIONFACTOR)
                           , RMSE = myRMSE(EXPANSIONFACTOR, EXP, N))
fname = file.path(validation_dir, "uniformity.csv")
write.csv(u_analysis_puma, file = fname, row.names = FALSE)


### Finish