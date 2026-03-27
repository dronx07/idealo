# 📦 Amazon FBA Discord Monitor

A fully automated, production-ready **Amazon FBA deal monitor** that scans supplier products, calculates profitability, and posts premium embeds to Discord in scheduled batches. Designed for **GitHub Actions**, **async-safe**, and **ROI-based embeds** for a professional look.

---

## 📝 Features

* Daily scan of supplier products from GitHub JSON
* Automatic **EAN → ASIN conversion**
* Amazon Seller Central scraping:

  * Product title, price, fees
  * Estimated monthly sales
* Automatic **profit and ROI calculation**
* Filters:

  * ROI ≥ 25%
  * Profit ≥ €1
  * Estimated sales ≥ 5/month
* **Duplicate control** (EAN + ASIN)
* **Batch posting** to Discord for even daily distribution
* **Premium Discord embeds** with:

  * Product image
  * Financial details (profit, ROI, fees)
  * Supplier, Amazon, and SAS links
  * ROI-based colors (green → high ROI, gold → medium ROI, red → low ROI)
* Randomized posting delay for natural behavior
* Async-safe JSON-based database
* GitHub Actions compatible
