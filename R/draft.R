library(tidyverse)
library(data.table)
library(janitor)
library(pointblank)
if (!fs::file_exists("data.zip")) {
curl::curl_download(
"https://github.com/eribul/cs/raw/refs/heads/main/data.zip",
"data.zip",
quiet = FALSE
)
}
 library(tidyverse)
+ library(data.table)
+ patients <-
+ readr::read_csv(unz("data.zip", "data-fixed/patients.csv")) |>
+ setDT() |>
+ setkey(id)
# read_csv() with unz() to read without unzipping and setDt for data.table format
patients <- janitor::remove_empty(patients, quiet = FALSE)
patients <- janitor::remove_constant(patients, quiet = FALSE)
# remove empty rows and redundant values
checks <-
  patients |>
  create_agent(label = "ECS3 Patient Data Validation") |>
  col_vals_between(
    where(is.Date),
    as.Date("1900-01-01"),
    as.Date(Sys.Date()),
    na_pass = TRUE,
    label = "All date values must fall between 1900-01-01 and today"
  ) |>
  col_vals_gte(
    deathdate,
    vars(birthdate),
    na_pass = TRUE,
    label = "Death date must be on or after birth date"
  ) |>
  col_vals_regex(
    ssn,
    "[0-9]{3}-[0-9]{2}-[0-9]{4}$",
    label = "SSN must follow US format: 3 digits - 2 digits - 4 digits (e.g. 123-45-6789)"
  ) |>
  col_is_integer(
    id,
    label = "Patient ID must be an integer (whole number)"
  ) |>
    col_vals_gte(
    birthdate,
    value = as.Date("1900-01-01"),
    na_pass = TRUE,
    label = "No patient born before 1900"
  ) |>
  col_vals_in_set(
    gender,
    set = c("M", "F"),
    label = "Gender must be either M or F only"
  ) |>
  interrogate()
checks
#Comments: define the way we need our  data to look like
export_report(checks, "patient_validation.html")
patients[, .N, marital]
patients[,
  marital := factor(
    marital,
    levels = c("S", "M", "D", "W"),
    labels = c("Single", "Married", "Divorced", "Widowed")
  )
]
fctr_candidates <-
  patients[, which(lapply(.SD, uniqueN) < 10), .SDcols = is.character] |>
  names()
patients[,
  lapply(.SD, \(x) paste(unique(x), collapse = ", ")),
  .SDcols = fctr_candidates
] |>
  glimpse()
patients[,
  names(.SD) := lapply(.SD, as.factor),
  .SDcols = c("gender", "race", "ethnicity")
]
#Converted categorical variables to factor
patients[, .N, .(gender, race, state)][order(N)]
patients[, race := forcats::fct_lump_prop(race, prop = 0.05)]
# Merge rare race categories (less than 5% of total) into "Other"
unzip("data.zip", files = "data-fixed/payer_transitions.csv")
lastdate <-
  duckplyr::read_csv_duckdb("data-fixed/payer_transitions.csv") |>
  summarise(lastdate = max(start_date)) |>
  collect() |>
  pluck("lastdate") |>
  as.Date()
lastdate
patients[, age := as.integer((as.IDate(lastdate) - as.IDate(birthdate))) %/% 365.241]
#Use strt date as proxy for calculating age
patients[is.na(deathdate), hist(age,
  main = "Age distribution of living patients at data collection",
  xlab = "Age",
  col = "steelblue"
)]
#age distribution histogram
patients[,
  names(.SD) := lapply(.SD, \(x) replace_na(x, "")),
  .SDcols = c("prefix", "middle")
]
patients[,
  full_name := paste(
    prefix,
    first,
    middle,
    last,
    fifelse(!suffix %in% c("", NA), paste0(", ", suffix), "")
  )
]
patients[, names(.SD) := lapply(.SD, trimws), .SDcols = is.character]
patients[, full_name := stringr::str_replace_all(full_name, " +", " ")]
patients[, c("prefix", "first", "middle", "last", "suffix", "maiden") := NULL]
#Comment: to address patients in personalised letters
patients[, driver := !is.na(drivers)][, drivers := NULL]
leaflet::leaflet(data = patients) |>
  leaflet::addTiles() |>
  leaflet::addMarkers(~lon, ~lat, label = ~full_name)
# patient locations mapped on to the US, data only from US. 
zip::unzip("data.zip")
fs::dir_create("data-parquet")
csv2parquet <- function(file) {
  new_file <-
    file |>
    stringr::str_replace("-fixed", "-parquet") |>
    stringr::str_replace(".csv", ".parquet")
  duckplyr::read_csv_duckdb(file) |>
    duckplyr::compute_parquet(new_file)
}
fs::dir_ls("data-fixed/") |>
  purrr::walk(csv2parquet, .progress = TRUE)
fs::dir_delete("data-fixed")
procedures <- duckplyr::read_parquet_duckdb("data-parquet/procedures.parquet") |>
  select(patient, reasoncode_icd10, start) |>
  filter(!is.na(reasoncode_icd10)) |>  # Only keep rows WITH a reason
  collect()
setDT(procedures, key = "patient")
procedures[, year := year(start)][, start := NULL]
proc_n_adults <-
  procedures[
    patients[, .(id, birthdate = as.IDate(birthdate))],
    on = c(patient = "id")
  ] |>
  _[year - year(birthdate) >= 18L, .N, .(reasoncode_icd10, year)]
cond_by_year <- setDT(decoder::icd10se)[
  proc_n_adults,
  on = c(key = "reasoncode_icd10")
]
#paraquet files compressed and easier to read. 
top5 <- cond_by_year[, .(N = sum(N)), .(value)][order(-N)][1:5, value]
ggplot(cond_by_year[.(top5), on = "value"], aes(year, N, color = value)) +
  geom_line() +
  labs(
    title = "Top 5 procedure reasons in adults over time",
    x = "Year",
    y = "Number of procedures",
    color = "Condition"
  ) +
  theme_minimal() +
  theme(legend.position = "bottom") +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_discrete(
    labels = function(x) str_wrap(x, width = 40)
  )
# First years have very little data and last year collection could have been ongoing.
# Data might have been sparse in the early periods so better to focus on the years with more relevant data.
# An increase in numbers could always be due to more recorded treatments or more hospitalizations. Need not be due to more disease occurence.
# Standardization with population size necessary to know the prevalent rates of the disease now. 
# Patients for diseases that was undertreated in the past could make it look like the disease prevalence is more at present however, this could also mean 
# more treatment records. To be sure standardization with population size essential.  