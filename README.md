# Otto's Stuff Repository

A collection of data tools, utilities, and projects for data engineering, analytics, and experimentation.

## 📁 Projects

### 🛒 [E-commerce Data Generator](./ecommerce-data-generator/)
A comprehensive toolkit for generating realistic e-commerce datasets including:
- **Website traffic/session data** with realistic conversion patterns
- **Customer accounts database** with segmentation and behavioral data  
- **Email matching system** for customer identification across datasets
- **Asia-Pacific geographic focus**: Australia, Japan, Hong Kong, Singapore

**Features:**
- Low conversion rates (2-5%) and high cart abandonment (~70%)
- Guest checkout dominance (90%+) with account holder tracking
- Comprehensive matching analysis (15-25% match rates)
- Built with mimesis and pandas for fast, realistic data generation

[➡️ View E-commerce Data Generator Documentation](./ecommerce-data-generator/README.md)

---

### 🧠 [Model Jobs - MLOps Framework](./model-jobs/)
A production-ready MLOps framework for machine learning model training, scoring, and deployment in Databricks environments.
- **Databricks Integration**: Native support for Spark, Delta Lake, and MLflow
- **Scalable Processing**: Distributed training and batch inference
- **PyFunc Models**: Simple parameter-based models for demonstrations
- **Enterprise-Grade**: Widgets, monitoring, and comprehensive logging

**Key Components:**
- **Training Notebook**: Interactive model training with 10K samples and MLflow tracking
- **Scoring Notebook**: Distributed batch inference for 50K+ records using Spark UDFs
- **Delta Lake Storage**: Versioned data with partitioning and optimization
- **Performance Monitoring**: Real-time throughput and data quality metrics

**Technologies:** Databricks, MLflow, Spark, Delta Lake, Python, PyFunc Models

[➡️ View Model Jobs Documentation](./model-jobs/README.md)

---

## 🎯 Use Cases

### Data Generation & Analytics
- **E-commerce Analytics**: Generate realistic customer and transaction datasets
- **A/B Testing**: Create synthetic user behavior data for experimentation
- **Data Pipeline Testing**: Validate ETL processes with known data patterns

### MLOps & Machine Learning
- **Model Training**: End-to-end ML training workflows with experiment tracking
- **Batch Scoring**: Scalable inference pipelines for large datasets  
- **Model Monitoring**: Production-grade monitoring and quality assurance
- **Databricks Deployment**: Ready-to-use notebooks for cloud ML platforms

## 🚀 Quick Start

Each project has its own documentation and setup instructions:

1. **For E-commerce Data**: Navigate to \`./ecommerce-data-generator/\` and follow the setup guide
2. **For MLOps Framework**: Navigate to \`./model-jobs/\` and import notebooks to Databricks

## 🛠️ Technologies Used

| Project | Technologies |
|---------|-------------|
| **E-commerce Data Generator** | Python, Mimesis, Pandas, Git |
| **Model Jobs (MLOps)** | Databricks, MLflow, Spark, Delta Lake, PyFunc |

## 🤝 Contributing

Feel free to contribute improvements, new projects, or bug fixes:

1. Fork the repository
2. Create a feature branch (\`git checkout -b feature/amazing-feature\`)
3. Commit your changes (\`git commit -m 'Add amazing feature'\`)
4. Push to the branch (\`git push origin feature/amazing-feature\`)
5. Open a Pull Request

## 📜 License

Projects in this repository are licensed under the MIT License unless otherwise specified.

---

*Repository maintained by [Otto Li](https://github.com/otto-li)*
