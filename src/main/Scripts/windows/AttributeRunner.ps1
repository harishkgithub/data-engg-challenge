<#
.SYNOPSIS
	Runs Attribute applictaion to generate reports 
.DESCRIPTION
	This PowerShell script runs Attribute applictaion to generat reports.
.PARAMETER message
	Specifies the alert message
.EXAMPLE
	PS> ./AttributeRunner.ps1 "Harddisk failure"
.LINK
	https://github.com/
.NOTES
	Author: Harish Kumar 
#>

echo "Navigating to source code"
CD ../../../
PWD
echo "Compiling the project"
sbt package 
echo "Running Attribution application for report"
spark-submit --master local[1] .\target\scala-2.12\data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar  file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\events.csv file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\impressions.csv file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_events