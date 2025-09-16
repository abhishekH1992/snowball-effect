# Rockshop Click & Collect – UI/UX Implementation

## Overview
This project implements the **Click & Collect** functionality for [rockshop.co.nz](https://rockshop.co.nz), enabling customers to view real-time product availability across stores and seamlessly select their preferred pickup location. The solution covers both **frontend** and **backend** logic, ensuring a smooth and transparent shopping experience.

---

## Technology Stack
- **Frontend:** Vue.js  
- **Styling:** Tailwind CSS  
- **Backend:** Custom APIs for real-time stock management and store availability checks  

---

## Key Features

### 1. Real-time Stock Availability
- The system checks **live inventory data** across all Rockshop branches.  
- Each store displays how many items are available out of the total required.  
- Disabled store options indicate **no stock available** for the selected products.

### 2. Multi-product Cart Handling
- If multiple items are in the cart:
  - The UI shows **stock availability per product** under each store.  
  - When at least one product is unavailable in a store, a **warning message** is shown.  
  - Users are informed upfront whether all cart items can be collected from the selected store.  

### 3. Store Selection & Checkout
- Customers can select a store from the **Click & Collect modal**.  
- If a store without all products is selected:
  - At checkout, a **popup alert** notifies the user that some items are not available at the chosen store.  
  - Options are provided to **Change Store**, switch to **Delivery**, or **Continue** (removing unavailable items).  

### 4. Quantity Management
- Users can adjust product **quantities** directly:
  - In the main cart.  
  - On the store selection modal.  
- Stock availability dynamically updates with each change.

### 5. Real-time Pricing Updates
- Cart subtotal updates instantly when:
  - Quantities are changed.  
  - Items are removed or added.  
- Delivery/Click & Collect fees update dynamically as well.  

### 6. Stock Level Indicators
- **Low Stock** warnings are shown when product quantities approach depletion.  
- Products unavailable in a store are clearly marked.  

---

## Steps to Access
1. Go to [rockshop.co.nz](https://rockshop.co.nz)  
2. Add 2–3 products to the cart  
3. Go to the cart  
4. Click on **“Click & Collect”**  

---

## User Flow
1. User adds products to the cart.  
2. At checkout, selects **Click & Collect** or **Delivery**.  
3. If **Click & Collect** is chosen:  
   - A modal displays all stores with stock availability per product.  
   - User selects a preferred store.  
4. On checkout:  
   - If all products are available → proceed.  
   - If not → popup informs user with options to **Change Store**, **Switch to Delivery**, or **Continue** with available items only.  

---

## Screens Implemented
- **Cart Page** with product list, stock status, and pricing.  
- **Store Selection Modal** showing real-time availability across stores.  
- **Checkout Page** with Click & Collect logic.  
- **Error/Alert Popups** when items are unavailable.  

---

## Benefits
- Provides **transparency** to customers regarding product availability.  
- Reduces **friction** by handling multi-product stock conflicts upfront.  
- Ensures **accurate real-time pricing and delivery costs**.  
- Enhances customer satisfaction with clear, predictable Click & Collect workflows.  
