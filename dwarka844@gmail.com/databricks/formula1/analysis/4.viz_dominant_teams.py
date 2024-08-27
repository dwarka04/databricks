# Databricks notebook source
html = """<h1 style ="color:Black;text-align:center;font-family:ariel">Report on Dominant Formula 1 Teams </h1>"""
displayHTML(html)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace TEMP VIEW v_dominant_teams
# MAGIC as
# MAGIC select team_name,
# MAGIC count(1) as total_races,
# MAGIC sum(calculated_points)as total_points,
# MAGIC avg(calculated_points)as avg_points ,
# MAGIC rank() over (order by avg(calculated_points) desc) as team_rank
# MAGIC from f1_presentation.calculated_race_results
# MAGIC
# MAGIC group by team_name
# MAGIC having count(1) >= 100
# MAGIC order by avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year,
# MAGIC team_name,
# MAGIC count(1) as total_races,
# MAGIC sum(calculated_points)as total_points,
# MAGIC avg(calculated_points)as avg_points 
# MAGIC from f1_presentation.calculated_race_results
# MAGIC where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
# MAGIC group by race_year,team_name
# MAGIC
# MAGIC order by race_year,avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year,
# MAGIC team_name,
# MAGIC count(1) as total_races,
# MAGIC sum(calculated_points)as total_points,
# MAGIC avg(calculated_points)as avg_points 
# MAGIC from f1_presentation.calculated_race_results
# MAGIC where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
# MAGIC group by race_year,team_name
# MAGIC
# MAGIC order by race_year,avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year,
# MAGIC team_name,
# MAGIC count(1) as total_races,
# MAGIC sum(calculated_points)as total_points,
# MAGIC avg(calculated_points)as avg_points 
# MAGIC from f1_presentation.calculated_race_results
# MAGIC where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
# MAGIC group by race_year,team_name
# MAGIC
# MAGIC order by race_year,avg_points desc
