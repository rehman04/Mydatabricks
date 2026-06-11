# Databricks notebook source
# downstream.py
# Task 2 in shifaJOb — runs after the salespipeline pipeline task completes.
# Reads the refreshed sales_stats materialized view and signals downstream consumers.

print("📡 Calling downstream apps with fresh sales_stats data")
