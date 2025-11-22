#!/bin/bash
./../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./model/train_model.py
