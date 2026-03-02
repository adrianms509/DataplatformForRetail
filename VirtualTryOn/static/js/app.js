/**
 * Virtual Try-On Frontend Application
 */

class VirtualTryOnApp {
    constructor() {
        this.selectedItems = new Map(); // Map of productId -> product data
        this.currentCategory = 'hosen';
        this.products = {};
        this.generatedLook = null;
        this.cart = []; // Shopping cart items

        this.init();
    }

    async init() {
        this.bindElements();
        this.bindEvents();
        await this.loadProducts(this.currentCategory);
        
        // Restore cart from local storage
        this.restoreCart();
        
        // Restore last generated look from session storage
        this.restoreLastLook();
    }

    restoreCart() {
        const savedCart = localStorage.getItem('shoppingCart');
        if (savedCart) {
            try {
                this.cart = JSON.parse(savedCart);
                this.updateCartBadge();
            } catch (e) {
                console.error('Error restoring cart:', e);
                localStorage.removeItem('shoppingCart');
            }
        }
    }

    saveCart() {
        localStorage.setItem('shoppingCart', JSON.stringify(this.cart));
        this.updateCartBadge();
    }

    updateCartBadge() {
        const badges = document.querySelectorAll('.cart-badge');
        // Count total items across all looks
        const totalItems = this.cart.reduce((sum, look) => sum + look.items.length, 0);
        badges.forEach(badge => {
            if (totalItems > 0) {
                badge.textContent = totalItems;
                badge.style.display = 'flex';
            } else {
                badge.style.display = 'none';
            }
        });
    }

    restoreLastLook() {
        const savedLook = sessionStorage.getItem('lastGeneratedLook');
        const savedProducts = sessionStorage.getItem('lastSelectedProducts');
        
        if (savedLook && savedProducts) {
            try {
                const look = JSON.parse(savedLook);
                const products = JSON.parse(savedProducts);
                
                // Restore selected items
                products.forEach(product => {
                    this.selectedItems.set(product.id, product);
                });
                
                this.generatedLook = look;
                this.updateUI();
                this.showResult(look);
            } catch (e) {
                console.error('Error restoring last look:', e);
                sessionStorage.removeItem('lastGeneratedLook');
                sessionStorage.removeItem('lastSelectedProducts');
            }
        }
    }

    saveLookToSession(result) {
        sessionStorage.setItem('lastGeneratedLook', JSON.stringify(result));
        sessionStorage.setItem('lastSelectedProducts', JSON.stringify(Array.from(this.selectedItems.values())));
    }

    clearSavedLook() {
        sessionStorage.removeItem('lastGeneratedLook');
        sessionStorage.removeItem('lastSelectedProducts');
    }

    bindElements() {
        this.productsGrid = document.getElementById('productsGrid');
        this.selectedItemsContainer = document.getElementById('selectedItems');
        this.selectionHint = document.getElementById('selectionHint');
        this.btnGenerate = document.getElementById('btnGenerate');
        this.resultModal = document.getElementById('resultModal');
        this.dropZone = document.querySelector('.selection-section');
        
        // Setup drop zone
        this.setupDropZone();
        this.cartModal = document.getElementById('cartModal');
        this.loadingOverlay = document.getElementById('loadingOverlay');
        this.generatedLookImage = document.getElementById('generatedLookImage');
        this.lookDetails = document.getElementById('lookDetails');
        this.navItems = document.querySelectorAll('.nav-item');
    }

    bindEvents() {
        // Category navigation
        this.navItems.forEach(item => {
            item.addEventListener('click', () => this.switchCategory(item.dataset.category));
        });

        // Generate button
        this.btnGenerate.addEventListener('click', () => this.generateLook());

        // Modal close/customize
        document.getElementById('btnCustomize').addEventListener('click', () => this.closeModal());
        document.getElementById('btnAddToCart').addEventListener('click', () => this.addToCart());

        // Cart button
        document.querySelectorAll('.cart-btn').forEach(btn => {
            btn.addEventListener('click', () => this.openCart());
        });
        
        // Cart modal close and clear
        document.getElementById('closeCartBtn')?.addEventListener('click', () => this.closeCart());
        document.getElementById('clearCartBtn')?.addEventListener('click', () => this.clearCart());
        document.getElementById('continueShoppingBtn')?.addEventListener('click', () => this.closeCart());
        
        // Checkout
        document.getElementById('checkoutBtn')?.addEventListener('click', () => this.showCheckout());
        document.getElementById('closeCheckoutBtn')?.addEventListener('click', () => this.closeCheckout());
        document.getElementById('closeCheckoutX')?.addEventListener('click', () => this.closeCheckout());
    }

    async loadProducts(category) {
        try {
            const response = await fetch(`/api/products/${category}`);
            if (!response.ok) throw new Error('Failed to load products');
            
            const products = await response.json();
            this.products[category] = products;
            this.renderProducts(products);
        } catch (error) {
            console.error('Error loading products:', error);
            this.productsGrid.innerHTML = '<p class="error">Produkte konnten nicht geladen werden.</p>';
        }
    }

    renderProducts(products) {
        this.productsGrid.innerHTML = products.map(product => this.createProductCard(product)).join('');
        
        // Bind drag events to product cards
        this.productsGrid.querySelectorAll('.product-card').forEach(card => {
            card.setAttribute('draggable', 'true');
            
            card.addEventListener('dragstart', (e) => {
                const productId = card.dataset.productId;
                e.dataTransfer.setData('text/plain', productId);
                e.dataTransfer.effectAllowed = 'copy';
                card.classList.add('dragging');
                
                // Create a custom drag image
                const img = card.querySelector('.product-image');
                if (img) {
                    e.dataTransfer.setDragImage(img, 50, 50);
                }
            });
            
            card.addEventListener('dragend', () => {
                card.classList.remove('dragging');
            });
        });
    }

    getProductImageUrl(product) {
        // Get category from product ID (e.g., "hosen-1" -> "hosen")
        const category = product.id.split('-')[0];
        return `/static/products/${category}/${product.image}`;
    }

    getColorHex(colorName) {
        // Map German color names to hex values
        const colorMap = {
            'Schwarz': '#1a1a1a',
            'Weiß': '#f5f5f5',
            'Blau': '#2563eb',
            'Dunkelblau': '#1e3a5f',
            'Indigoblau': '#3f51b5',
            'Beige': '#d4b896',
            'Creme': '#fffdd0',
            'Braun': '#8b4513',
            'Koralle': '#ff7f50',
            'Orange': '#ff6b35',
            'Bunt': 'linear-gradient(135deg, #ff6b6b, #4ecdc4, #ffe66d)',
            'Grau': '#808080',
            'Rosa': '#ffb6c1',
            'Rot': '#dc2626',
            'Grün': '#22c55e'
        };
        return colorMap[colorName] || '#ccc';
    }

    createProductCard(product) {
        const isSelected = this.selectedItems.has(product.id);
        const hasDiscount = product.discount && product.originalPrice;
        const category = product.id.split('-')[0];
        
        const imageUrl = this.getProductImageUrl(product);

        return `
            <div class="product-card ${isSelected ? 'selected' : ''}" data-product-id="${product.id}" data-category="${category}">
                <div class="product-image-container">
                    <img src="${imageUrl}" 
                         alt="${product.name}" 
                         class="product-image">
                    ${hasDiscount ? `<span class="product-badge">-${product.discount}%</span>` : ''}
                    ${!hasDiscount && product.id.includes('-1') ? '<span class="product-badge" style="background: transparent; color: #1a1a1a; font-size: 0.65rem;">Online Special</span>' : ''}
                    <button class="product-wishlist" aria-label="Zur Wunschliste hinzufügen">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"></path>
                        </svg>
                    </button>
                </div>
                <div class="product-info">
                    <div class="product-brand">${product.brand}</div>
                    <div class="product-name">${product.name}</div>
                    ${product.color ? `<div class="product-color"><span class="color-dot" style="background-color: ${this.getColorHex(product.color)}"></span>${product.color}</div>` : ''}
                    <div class="product-price">
                        ${hasDiscount ? `<span class="price-original">${product.originalPrice.toFixed(2).replace('.', ',')} €</span>` : ''}
                        <span class="price-current ${hasDiscount ? 'price-sale' : ''}">${product.price.toFixed(2).replace('.', ',')} €</span>
                    </div>
                </div>
            </div>
        `;
    }

    setupDropZone() {
        // Prevent default drag behaviors
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            this.dropZone.addEventListener(eventName, (e) => {
                e.preventDefault();
                e.stopPropagation();
            });
        });
        
        // Highlight drop zone when dragging over
        ['dragenter', 'dragover'].forEach(eventName => {
            this.dropZone.addEventListener(eventName, () => {
                this.dropZone.classList.add('drop-zone-active');
            });
        });
        
        ['dragleave', 'drop'].forEach(eventName => {
            this.dropZone.addEventListener(eventName, () => {
                this.dropZone.classList.remove('drop-zone-active');
            });
        });
        
        // Handle drop
        this.dropZone.addEventListener('drop', (e) => {
            const productId = e.dataTransfer.getData('text/plain');
            if (productId) {
                this.addProductById(productId);
            }
        });
    }

    addProductById(productId) {
        // Already selected? Skip
        if (this.selectedItems.has(productId)) return;
        
        // Find the product in any category
        const category = productId.split('-')[0];
        let product = this.products[category]?.find(p => p.id === productId);
        
        // If not loaded yet, try current category
        if (!product) {
            product = this.products[this.currentCategory]?.find(p => p.id === productId);
        }
        
        if (product) {
            product.category = category;
            this.selectedItems.set(productId, product);
            this.updateUI();
        }
    }

    toggleProductSelection(productId) {
        // Find the product in current category
        const product = this.products[this.currentCategory]?.find(p => p.id === productId);
        if (!product) return;

        if (this.selectedItems.has(productId)) {
            this.selectedItems.delete(productId);
        } else {
            // Add product with category info for image path
            product.category = product.id.split('-')[0];
            this.selectedItems.set(productId, product);
        }

        this.updateUI();
    }

    removeSelectedItem(productId) {
        this.selectedItems.delete(productId);
        this.updateUI();
    }

    updateUI() {
        // Update product cards selection state
        this.productsGrid.querySelectorAll('.product-card').forEach(card => {
            const isSelected = this.selectedItems.has(card.dataset.productId);
            card.classList.toggle('selected', isSelected);
        });

        // Update selected items panel
        this.renderSelectedItems();

        // Update generate button state
        this.btnGenerate.disabled = this.selectedItems.size === 0;

        // Show/hide hint
        this.selectionHint.style.display = this.selectedItems.size === 0 ? 'block' : 'none';
    }

    renderSelectedItems() {
        if (this.selectedItems.size === 0) {
            this.selectedItemsContainer.innerHTML = '';
            return;
        }

        this.selectedItemsContainer.innerHTML = Array.from(this.selectedItems.values())
            .map(product => this.createSelectedItemCard(product))
            .join('');

        // Bind remove buttons
        this.selectedItemsContainer.querySelectorAll('.selected-item-remove').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.removeSelectedItem(btn.dataset.productId);
            });
        });
    }

    createSelectedItemCard(product) {
        const imageUrl = this.getProductImageUrl(product);
        const hasDiscount = product.discount && product.originalPrice;

        return `
            <div class="selected-item">
                <img src="${imageUrl}" 
                     alt="${product.name}" 
                     class="selected-item-image">
                ${hasDiscount ? `<span class="selected-item-badge">-${product.discount}%</span>` : ''}
                <button class="selected-item-remove" data-product-id="${product.id}" aria-label="Entfernen">×</button>
                <button class="selected-item-wishlist" aria-label="Zur Wunschliste">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"></path>
                    </svg>
                </button>
                <span class="selected-item-brand">${product.brand}</span>
            </div>
        `;
    }

    switchCategory(category) {
        if (category === this.currentCategory) return;

        this.currentCategory = category;

        // Update nav active state
        this.navItems.forEach(item => {
            item.classList.toggle('active', item.dataset.category === category);
        });

        // Load products for new category
        if (this.products[category]) {
            this.renderProducts(this.products[category]);
        } else {
            this.loadProducts(category);
        }
    }

    async generateLook() {
        if (this.selectedItems.size === 0) return;

        this.showLoading();

        try {
            const items = Array.from(this.selectedItems.values());
            const itemIds = Array.from(this.selectedItems.keys());
            
            // Start image generation (may return cached result)
            const generatePromise = fetch('/api/generate', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    items: itemIds
                })
            });
            
            // Wait for image generation (or cache hit)
            const response = await generatePromise;

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.error || 'Failed to generate look');
            }

            const result = await response.json();
            this.generatedLook = result;
            
            // Log cache status
            if (result.cached) {
                console.log('⚡ Loaded from cache:', result.combination_id);
            } else {
                console.log('🎨 Generated new look:', result.combination_id);
            }
            
            // Only send to Fabric if this is a NEW combination (not cached)
            if (!result.cached) {
                this.sendCombinationToFabric(items)
                    .then(combinationResult => {
                        if (combinationResult?.combination_id) {
                            this.generatedLook.combination_id = combinationResult.combination_id;
                            this.saveLookToSession(this.generatedLook);
                            console.log('✅ Combination saved to Fabric:', combinationResult.combination_id);
                        }
                    })
                    .catch(err => console.warn('Could not save combination to Fabric:', err));
            }
            
            this.saveLookToSession(result);
            this.showResult(result);
        } catch (error) {
            console.error('Error generating look:', error);
            alert('Fehler beim Generieren des Looks: ' + error.message);
        } finally {
            this.hideLoading();
        }
    }

    async sendCombinationToFabric(items) {
        try {
            const response = await fetch('/api/combination', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    user_id: this.getUserId(),
                    items: items
                })
            });
            return await response.json();
        } catch (error) {
            console.warn('Failed to send combination to Fabric:', error);
            return null;
        }
    }

    getUserId() {
        // Get or create a persistent user ID
        let userId = localStorage.getItem('virtualTryOnUserId');
        if (!userId) {
            userId = 'user-' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('virtualTryOnUserId', userId);
        }
        return userId;
    }

    showResult(result) {
        // Set generated image
        this.generatedLookImage.src = result.image;

        // Render product details with size selection
        this.lookDetails.innerHTML = Array.from(this.selectedItems.values())
            .map(product => this.createLookItemCard(product))
            .join('');

        // Show modal
        this.resultModal.classList.add('active');
    }

    createLookItemCard(product) {
        const hasDiscount = product.discount && product.originalPrice;
        const deliveryDate = this.getDeliveryDate();

        return `
            <div class="look-item">
                <div class="look-item-name">${product.name}</div>
                <div class="look-item-delivery">
                    Lieferung ca. <strong>${deliveryDate}</strong>
                </div>
                <div class="look-item-price">
                    ${hasDiscount ? `<span class="price-original">${product.originalPrice.toFixed(2).replace('.', ',')} €</span>` : ''}
                    <span class="price-current ${hasDiscount ? 'price-sale' : ''}">${product.price.toFixed(2).replace('.', ',')} €</span>
                    <span style="color: #666; font-size: 0.8rem;">Preis inkl. MwSt.</span>
                </div>
                ${product.brand ? `<div class="look-item-brand">${product.brand}</div>` : ''}
                <div class="size-select-row">
                    <select class="size-select" data-product-id="${product.id}">
                        <option value="">GRÖSSE</option>
                        ${product.sizes.map(size => `<option value="${size}">${size}</option>`).join('')}
                    </select>
                    <button class="btn-add-item" aria-label="In den Warenkorb">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4Z"></path>
                            <path d="M3 6h18"></path>
                            <path d="M16 10a4 4 0 0 1-8 0"></path>
                        </svg>
                    </button>
                </div>
            </div>
        `;
    }

    getDeliveryDate() {
        const today = new Date();
        const start = new Date(today);
        start.setDate(start.getDate() + 2);
        const end = new Date(today);
        end.setDate(end.getDate() + 4);

        const options = { weekday: 'long', day: 'numeric', month: 'long' };
        const startStr = start.toLocaleDateString('de-DE', options);
        const endStr = end.toLocaleDateString('de-DE', { day: 'numeric', month: 'long' });

        return `${startStr} - ${endStr}`;
    }

    closeModal() {
        this.resultModal.classList.remove('active');
        this.clearSavedLook();
    }

    addToCart() {
        // Check if all sizes are selected
        const sizeSelects = this.lookDetails.querySelectorAll('.size-select');
        let allSizesSelected = true;
        const selectedSizes = {};
        
        sizeSelects.forEach(select => {
            if (!select.value) {
                allSizesSelected = false;
                select.style.borderColor = '#c00';
            } else {
                select.style.borderColor = '#e5e5e5';
                selectedSizes[select.dataset.productId] = select.value;
            }
        });

        if (!allSizesSelected) {
            this.showToast('Bitte wählen Sie für alle Artikel eine Größe aus.', 'warning');
            return;
        }

        // Add items to cart with selected sizes
        const lookId = Date.now(); // Unique ID for this look
        const lookItems = Array.from(this.selectedItems.values()).map(product => ({
            ...product,
            selectedSize: selectedSizes[product.id],
            lookId: lookId,
            lookImage: this.generatedLook?.image
        }));
        
        this.cart.push({
            id: lookId,
            items: lookItems,
            image: this.generatedLook?.image,
            combination_id: this.generatedLook?.combination_id, // Include Fabric combination ID
            addedAt: new Date().toISOString()
        });
        
        this.saveCart();
        
        this.showToast('Look wurde dem Warenkorb hinzugefügt!');
        this.closeModal();
        this.clearSavedLook();
        
        // Reset selection
        this.selectedItems.clear();
        this.updateUI();
    }

    openCart() {
        this.renderCart();
        this.cartModal.classList.add('active');
    }

    closeCart() {
        this.cartModal.classList.remove('active');
    }

    clearCart() {
        if (confirm('Möchten Sie den Warenkorb wirklich leeren?')) {
            this.cart = [];
            this.saveCart();
            this.renderCart();
        }
    }

    removeFromCart(lookId) {
        this.cart = this.cart.filter(look => look.id !== lookId);
        this.saveCart();
        this.renderCart();
    }

    removeItemFromCart(lookId, itemId) {
        const look = this.cart.find(l => l.id === lookId);
        if (look) {
            look.items = look.items.filter(item => item.id !== itemId);
            // If no items left in look, remove the entire look
            if (look.items.length === 0) {
                this.cart = this.cart.filter(l => l.id !== lookId);
            }
        }
        this.saveCart();
        this.renderCart();
    }

    getCartTotal() {
        return this.cart.reduce((total, look) => {
            return total + look.items.reduce((lookTotal, item) => lookTotal + item.price, 0);
        }, 0);
    }

    renderCart() {
        const cartItems = document.getElementById('cartItems');
        const cartTotal = document.getElementById('cartTotal');
        const cartEmpty = document.getElementById('cartEmpty');
        
        if (!cartItems) return;

        if (this.cart.length === 0) {
            cartItems.innerHTML = '';
            if (cartEmpty) cartEmpty.style.display = 'block';
            if (cartTotal) cartTotal.textContent = '0,00 €';
            return;
        }

        if (cartEmpty) cartEmpty.style.display = 'none';

        cartItems.innerHTML = this.cart.map(look => `
            <div class="cart-look" data-look-id="${look.id}">
                <div class="cart-look-header">
                    <span class="cart-look-title">Look vom ${new Date(look.addedAt).toLocaleDateString('de-DE')}</span>
                    <button class="cart-look-remove" onclick="app.removeFromCart(${look.id})" title="Look entfernen">×</button>
                </div>
                <div class="cart-look-content">
                    <div class="cart-look-items-section">
                        <div class="cart-look-items">
                            ${look.items.map(item => {
                                const category = item.id ? item.id.split('-')[0] : '';
                                const thumbUrl = item.image ? `/static/products/${category}/${item.image}` : '';
                                return `
                                <div class="cart-item">
                                    ${thumbUrl ? `<img src="${thumbUrl}" alt="${item.name}" class="cart-item-thumb" onerror="this.style.display='none'">` : '<div class="cart-item-thumb"></div>'}
                                    <div class="cart-item-info">
                                        <span class="cart-item-name">${item.name}</span>
                                        <div class="cart-item-meta">
                                            <span class="cart-item-size">${item.selectedSize}</span>
                                            <span class="cart-item-brand">${item.brand}</span>
                                        </div>
                                    </div>
                                    <span class="cart-item-price">${item.price.toFixed(2).replace('.', ',')} €</span>
                                    <button class="cart-item-remove" onclick="app.removeItemFromCart(${look.id}, '${item.id}')" title="Artikel entfernen">×</button>
                                </div>
                            `}).join('')}
                        </div>
                        <div class="cart-look-subtotal">
                            <span class="cart-look-subtotal-label">Zwischensumme</span>
                            <span class="cart-look-subtotal-price">${look.items.reduce((sum, item) => sum + item.price, 0).toFixed(2).replace('.', ',')} €</span>
                        </div>
                    </div>
                    ${look.image ? `
                        <div class="cart-look-image-wrapper">
                            <img src="${look.image}" alt="Look" class="cart-look-image">
                        </div>
                    ` : ''}
                </div>
            </div>
        `).join('');

        if (cartTotal) {
            cartTotal.textContent = this.getCartTotal().toFixed(2).replace('.', ',') + ' €';
        }
    }

    showLoading() {
        this.loadingOverlay.classList.add('active');
    }

    hideLoading() {
        this.loadingOverlay.classList.remove('active');
    }

    showToast(message, type = 'success') {
        // Remove existing toast if any
        const existingToast = document.querySelector('.toast-notification');
        if (existingToast) {
            existingToast.remove();
        }
        
        const toast = document.createElement('div');
        toast.className = `toast-notification toast-${type}`;
        toast.innerHTML = `
            <div class="toast-icon">
                ${type === 'success' 
                    ? '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><path d="m9 12 2 2 4-4"></path></svg>'
                    : '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><path d="M12 8v4"></path><path d="M12 16h.01"></path></svg>'
                }
            </div>
            <span class="toast-message">${message}</span>
        `;
        
        document.body.appendChild(toast);
        
        // Trigger animation
        requestAnimationFrame(() => {
            toast.classList.add('show');
        });
        
        // Auto remove after 3 seconds
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }

    showCheckout() {
        if (this.cart.length === 0) return;
        
        // Collect all items and combination IDs from the cart
        const allItems = this.cart.flatMap(look => look.items);
        const combinationIds = this.cart.map(look => look.combination_id).filter(Boolean);
        
        // Send order to Fabric (non-blocking)
        this.sendOrderToFabric(allItems, combinationIds.length > 0 ? combinationIds[0] : null)
            .then(orderResult => {
                if (orderResult?.order_id) {
                    console.log('✅ Order sent to Fabric:', orderResult.order_id);
                }
            })
            .catch(err => console.warn('Could not send order to Fabric:', err));
        
        // Generate order number
        const orderNumber = Math.floor(100000 + Math.random() * 900000);
        document.getElementById('orderNumber').textContent = orderNumber;
        
        // Calculate delivery date (3-5 business days)
        const deliveryDate = new Date();
        deliveryDate.setDate(deliveryDate.getDate() + 3 + Math.floor(Math.random() * 3));
        const options = { weekday: 'long', day: 'numeric', month: 'long' };
        document.getElementById('deliveryDate').textContent = deliveryDate.toLocaleDateString('de-DE', options);
        
        // Render checkout summary
        const checkoutSummary = document.getElementById('checkoutSummary');
        const checkoutSubtotal = document.getElementById('checkoutSubtotal');
        const checkoutTotal = document.getElementById('checkoutTotal');
        
        const total = this.getCartTotal();
        
        if (checkoutSummary) {
            checkoutSummary.innerHTML = allItems.map(item => {
                const category = item.id ? item.id.split('-')[0] : '';
                const thumbUrl = item.image ? `/static/products/${category}/${item.image}` : '';
                return `
                    <div class="checkout-summary-item">
                        ${thumbUrl ? `<img src="${thumbUrl}" alt="${item.name}" class="checkout-summary-image">` : ''}
                        <div class="checkout-summary-details">
                            <div class="checkout-summary-name">${item.name}</div>
                            <div class="checkout-summary-brand">${item.brand}</div>
                        </div>
                        <span class="checkout-summary-price">${item.price.toFixed(2).replace('.', ',')} €</span>
                    </div>
                `;
            }).join('');
        }
        
        if (checkoutSubtotal) {
            checkoutSubtotal.textContent = total.toFixed(2).replace('.', ',') + ' €';
        }
        
        if (checkoutTotal) {
            checkoutTotal.textContent = total.toFixed(2).replace('.', ',') + ' €';
        }
        
        // Close cart and show checkout
        this.closeCart();
        document.getElementById('checkoutModal').classList.add('active');
        
        // Clear cart after "order"
        this.cart = [];
        this.saveCart();
    }

    async sendOrderToFabric(items, combinationId) {
        try {
            const response = await fetch('/api/order', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    user_id: this.getUserId(),
                    combination_id: combinationId,
                    items: items
                })
            });
            return await response.json();
        } catch (error) {
            console.warn('Failed to send order to Fabric:', error);
            return null;
        }
    }

    closeCheckout() {
        document.getElementById('checkoutModal').classList.remove('active');
    }
}

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.app = new VirtualTryOnApp();
});
