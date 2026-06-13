# K-Means Clustering Results

## Model Configuration

| Parameter           | Value   |
| ------------------- | ------- |
| Dataset Rows        | 419,883 |
| Dataset Columns     | 25      |
| Train Rows          | 336,139 |
| Test Rows           | 83,744  |
| Feature Vector Size | 5       |

## Selected Features

* Latitude
* Longitude
* Hour
* Location_Crime_Count
* PrimaryType_ArrestRate

## K Selection Evaluation

| K Value | Silhouette Score |
| ------- | ---------------- |
| 2       | 0.3304           |
| 3       | 0.3123           |
| 4       | 0.3217           |
| 5       | 0.3467           |
| 6       | 0.3235           |

## Best Model

* Best K = 5
* Final Silhouette Score = 0.3467
* Baseline Silhouette Score = 0.3304

## Cluster Distribution

| Cluster | Records |
| ------- | ------- |
| 0       | 11,708  |
| 1       | 23,235  |
| 2       | 14,644  |
| 3       | 20,069  |
| 4       | 14,088  |

## Conclusion

The K-Means model achieved its best clustering performance at K=5, outperforming the baseline configuration and successfully identifying meaningful crime patterns within the Chicago crime dataset.
