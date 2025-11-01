/*
 * TeaTrade Main Script
 *
 * This file is structured in a specific order:
 * 1. Global Variables & Helper Functions (Modals)
 * 2. Save & Dashboard Functions (The "logic" for saving items)
 * 3. Page-Specific Functions (displayProducts, displayRecipes, etc.)
 * 4. Page-Specific Initializers (Code that runs *only* on certain pages)
 * 5. Global Authentication Listener (This runs last and ties everything together)
 */

// ===================================
// === GLOBAL HELPER FUNCTIONS ===
// Moved outside DOMContentLoaded to be accessible
// by inline scripts (e.g., jobs.html)
// ===================================

// Updates the save button's visual state
function toggleSaveState(button, isSaved) {
    if (isSaved) {
        button.classList.add('saved');
        button.title = "Unsave item";
    } else {
        button.classList.remove('saved');
        button.title = "Save item";
    }
}

// Checks all cards on a page and updates their save state
async function loadAndUpdateSaveStatus(user, itemType) {
    // Need to get 'db' which is defined in the HTML
    if (!user || typeof db === 'undefined') {
        console.log("User or db not ready for loadAndUpdateSaveStatus");
        return; 
    }

    const savedItemIds = new Set();
    try {
        const snapshot = await db.collection('user_saves').doc(user.uid).collection(itemType).get();
        snapshot.forEach(doc => {
            savedItemIds.add(doc.id); // Add the safe btoa() ID
        });

        if (savedItemIds.size > 0) {
            const allButtons = document.querySelectorAll(`.save-btn[data-item-type="${itemType}"]`);
            allButtons.forEach(button => {
                if (savedItemIds.has(button.dataset.itemId)) {
                    toggleSaveState(button, true);
                }
            });
        }
    } catch (error) {
        console.error("Error loading saved statuses: ", error);
    }
}


document.addEventListener('DOMContentLoaded', () => {

    // ===================================
    // === 1. GLOBAL VARIABLES & HELPERS ===
    // ===================================
    let allProducts = []; // Stores products from products.json
    const modalOverlay = document.getElementById('info-modal');
    const modalContent = document.getElementById('modal-body-content');
    const closeModalBtn = document.getElementById('modal-close-btn');
    const productGrid = document.getElementById('product-grid'); // Main product grid

    // --- Modal Logic ---
    function openModal(title, content) {
        if (!modalOverlay || !modalContent) return;
        modalContent.innerHTML = `<h2>${title}</h2>${content}`;
        modalOverlay.classList.add('active');
    }

    function closeModal() {
        if (!modalOverlay) return;
        modalOverlay.classList.remove('active');
        modalContent.innerHTML = ''; // Clear content
    }

    if (closeModalBtn) {
        closeModalBtn.addEventListener('click', closeModal);
    }
    if (modalOverlay) {
        modalOverlay.addEventListener('click', (event) => {
            if (event.target === modalOverlay) {
                closeModal();
            }
        });
    }

    // ===================================
    // === 2. SAVE & DASHBOARD FUNCTIONS ===
    // These are defined first so they can be called by other functions
    // ===================================
    
    // *** toggleSaveState and loadAndUpdateSaveStatus are now global ***

    // Handles clicking a save button
    async function handleSaveClick(e, button, user) {
        // --- FIX: Stop the link click IMMEDIATELY ---
        e.preventDefault(); 
        e.stopPropagation(); // Stop the event from "bubbling" up to the link
        
        if (!user) {
            // --- FIX: Redirect to login if user is logged out ---
            const prefix = document.body.classList.contains('recipe-detail-page') ? '../' : './';
            window.location.href = `${prefix}login.html`;
            return;
        }
        if (typeof db === 'undefined') {
            console.error("Database (db) is not defined.");
            return;
        }

        const itemId = button.dataset.itemId; // This is the safe btoa() ID
        const itemType = button.dataset.itemType; // "products", "recipes", "jobs"
        const itemRef = db.collection('user_saves').doc(user.uid).collection(itemType).doc(itemId);

        try {
            const doc = await itemRef.get();
            if (doc.exists) {
                // --- Item is already saved, so UNSAVE it ---
                await itemRef.delete();
                toggleSaveState(button, false);
                console.log('Item unsaved!');

                // --- FIX: If we are on the dashboard, remove the card from the view ---
                if (document.body.classList.contains('dashboard-page')) {
                    const card = button.closest('.product-card, .recipe-card, .job-card');
                    if (card) {
                        card.remove();
                    }
                }

            } else {
                // --- Item is not saved, so SAVE it ---
                let itemData;
                if (itemType === 'products') {
                    // Find the product in the global 'allProducts' array
                    const originalLink = atob(itemId); // We must decode the ID (atob) to find the original link
                    itemData = allProducts.find(p => p.link === originalLink);
                } 
                else if (itemType === 'recipes') {
                    // Get data directly from the button's data attributes
                    itemData = {
                        name: button.dataset.name,
                        description: button.dataset.description,
                        image: button.dataset.image,
                        href: button.dataset.href
                    };
                }
                else if (itemType === 'jobs') {
                    itemData = {
                        id: atob(itemId), // Decode from btoa (e.g., "job1")
                        name: button.dataset.name,
                        company: button.dataset.company,
                        location: button.dataset.location
                    };
                }

                if (itemData) {
                    await itemRef.set(itemData); // Save the full product object
                    toggleSaveState(button, true);
                    console.log('Item saved!');
                } else {
                    console.warn('Could not find item data to save for ID:', itemId);
                }
            }
        } catch (error) {
            console.error("Error saving item: ", error);
        }
    }

    // This function runs once the user is confirmed to be logged in (or out)
    function initializeUserContent(user) {
        // Find all potential save-button containers
        const saveButtonContainers = document.querySelectorAll('.product-grid-container, .recipe-grid-container, .job-list-pane, #saved-jobs-grid');

        if (saveButtonContainers.length > 0) {
            // Add click listener for all save buttons on the page
            saveButtonContainers.forEach(container => {
                container.addEventListener('click', (e) => {
                    const saveBtn = e.target.closest('.save-btn');
                    if (saveBtn) {
                        // Pass the 'e' event object to handleSaveClick
                        handleSaveClick(e, saveBtn, user);
                    }
                });
            });
        }
        
        // If we are logged in, load save status and dashboard items
        if (user) {
            // If we're on the product page
            if (productGrid) {
                loadAndUpdateSaveStatus(user, 'products');
            }
            // If we're on the recipe page
            if (document.body.classList.contains('recipes-page') || document.body.classList.contains('recipe-detail-page')) {
                loadAndUpdateSaveStatus(user, 'recipes');
            }
            
            // If we're on the dashboard
            if (document.body.classList.contains('dashboard-page')) {
                loadSavedProducts(user);
                loadSavedRecipes(user); 
                loadSavedJobs(user); 
            }
        }
    }
    
    // ===================================
    // === 3. PAGE-SPECIFIC FUNCTIONS ===
    // (Display logic for products, recipes, etc.)
    // ===================================
    
    function displayProducts(products, containerId = 'product-grid') {
        const grid = document.getElementById(containerId);
        if (!grid) {
             return; 
        }

        grid.innerHTML = ''; // Clear "Loading..." message
        
        if (containerId === 'product-grid') {
            const oldSchemaScripts = document.querySelectorAll('script[type="application/ld+json"].product-schema');
            oldSchemaScripts.forEach(script => script.remove());
        }

        if (products.length === 0) {
            if (containerId === 'saved-products-grid') {
                 grid.innerHTML = '<p class="empty-message">You haven\'t saved any products yet. Click the save icon on a product to add it here!</p>';
            } else {
                 grid.innerHTML = '<p class="empty-message">No products match your filters.</p>';
            }
            return;
        }

        products.forEach(product => {
            const card = document.createElement('div');
            card.className = 'product-card';
            
            const safeId = btoa(product.link);
            card.dataset.productId = safeId; 

            const ratingPercent = (product.rating / 5) * 100;
            const priceDisplay = product.price.toLowerCase().includes('free') || product.price.includes('£') ? product.price : `£${product.price}`;

            let badgeHTML = '';
            if (product.rating >= 4.8 && product.reviewCount > 1000) {
                badgeHTML = '<span class="product-badge top-rated">Top Rated</span>';
            } else if (product.reviewCount > 5000) {
                 badgeHTML = '<span class="product-badge popular">Popular</span>';
            }

            card.innerHTML = `
                <a href="${product.link}" target="_blank" rel="noopener sponsored" class="product-link">
                    <div class="product-card-image-wrapper">
                        <button class="save-btn" title="Save product" data-item-id="${safeId}" data-item-type="products">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
                        </button>
                        ${badgeHTML}
                        <img src="${product.image}" alt="${product.name}" class="product-card-image" loading="lazy">
                    </div>
                    <div class="product-card-info">
                        <h3 class="product-name">${product.name}</h3>
                        <p class="product-brand">${product.brand}</p>
                        <p class="product-price">${priceDisplay}</p>
                    </div>
                </a>
                <div class="product-reviews">
                     ${product.rating ? `
                        <div> <div class="star-rating" title="${product.rating} out of 5 stars">
                                <div class="star-rating-filled" style="width: ${ratingPercent}%;"></div>
                            </div>
                            <span class="review-count">(${product.reviewCount.toLocaleString()})</span>
                        </div>
                    ` : '<div></div>'} <button class="quick-view-btn" aria-label="Quick view ${product.name}" data-product-link="${product.link}">
                        <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path><circle cx="12" cy="12" r="3"></circle></svg>
                    </button>
                </div>
            `;
            grid.appendChild(card);
            
            if (containerId === 'product-grid') {
                const schema = {
                    "@context": "https://schema.org/",
                    "@type": "Product",
                    "name": product.name, "image": product.image, "description": product.description,
                    "brand": { "@type": "Brand", "name": product.brand },
                    "sku": product.link, 
                    "offers": {
                        "@type": "Offer", "url": product.link, "priceCurrency": "GBP",
                        "price": isNaN(parseFloat(product.price)) ? "0" : parseFloat(product.price).toFixed(2), 
                        "availability": "https://schema.org/InStock", "itemCondition": "https://schema.org/NewCondition"
                    }
                };
                 if (product.rating && product.reviewCount) {
                    schema.aggregateRating = {
                        "@type": "AggregateRating",
                        "ratingValue": product.rating.toString(), "reviewCount": product.reviewCount.toString()
                    };
                }
                const scriptTag = document.createElement('script');
                scriptTag.type = 'application/ld+json';
                scriptTag.className = 'product-schema';
                scriptTag.textContent = JSON.stringify(schema, null, 2);
                document.head.appendChild(scriptTag);
            }
        });
    }

    function populateFilters(products) {
        const brandFilter = document.getElementById('filter-brand');
        const originFilter = document.getElementById('filter-origin');
        const formatFilter = document.getElementById('filter-format'); 
        const brands = new Set();
        const origins = new Set();
        const formats = new Set(); 

        products.forEach(product => {
            if (product.brand) brands.add(product.brand);
            if (product.origin) origins.add(product.origin);
            if (product.format) formats.add(product.format); 
        });

        if (brandFilter) {
            [...brands].sort().forEach(brand => {
                const option = document.createElement('option');
                option.value = brand.toLowerCase().replace(/\s+/g, '-');
                option.textContent = brand;
                brandFilter.appendChild(option);
            });
        }
        if (originFilter) {
            [...origins].sort().forEach(origin => {
                const option = document.createElement('option');
                option.value = origin.toLowerCase().replace(/\s+/g, '-');
                option.textContent = origin;
                originFilter.appendChild(option);
            });
        }
        if (formatFilter) {
            [...formats].sort().forEach(format => {
                const option = document.createElement('option');
                option.value = format.toLowerCase().replace(/\s+/g, '-');
                option.textContent = format.charAt(0).toUpperCase() + format.slice(1);
                formatFilter.appendChild(option);
            });
        }
    }
    
    function setupFiltering(products) {
        const filters = [
            document.getElementById('filter-type'),
            document.getElementById('filter-brand'),
            document.getElementById('filter-origin'),
            document.getElementById('filter-format'),
            document.getElementById('filter-sort')
        ];

        function applyFiltersAndSort() {
            const currentUser = (typeof auth !== 'undefined') ? auth.currentUser : null;
            const selectedType = filters[0] ? filters[0].value : 'all';
            const selectedBrand = filters[1] ? filters[1].value : 'all';
            const selectedOrigin = filters[2] ? filters[2].value : 'all';
            const selectedFormat = filters[3] ? filters[3].value : 'all'; 
            const selectedSort = filters[4] ? filters[4].value : 'default';

            let filteredProducts = products.filter(product => {
                const typeMatch = selectedType === 'all' || product.category === selectedType;
                const brandMatch = selectedBrand === 'all' || product.brand.toLowerCase().replace(/\s+/g, '-') === selectedBrand;
                const originMatch = selectedOrigin === 'all' || product.origin.toLowerCase().replace(/\s+/g, '-') === selectedOrigin;
                const formatMatch = selectedFormat === 'all' || (product.format && product.format.toLowerCase().replace(/\s+/g, '-') === selectedFormat);
                return typeMatch && brandMatch && originMatch && formatMatch; 
            });

            switch (selectedSort) {
                case 'price-asc':
                     filteredProducts.sort((a, b) => {
                        const priceA = isNaN(parseFloat(a.price)) ? 0 : parseFloat(a.price);
                        const priceB = isNaN(parseFloat(b.price)) ? 0 : parseFloat(b.price);
                        return priceA - priceB;
                    });
                    break;
                case 'price-desc':
                     filteredProducts.sort((a, b) => {
                        const priceA = isNaN(parseFloat(a.price)) ? 0 : parseFloat(a.price);
                        const priceB = isNaN(parseFloat(b.price)) ? 0 : parseFloat(b.price);
                        return priceB - priceA;
                    });
                    break;
                case 'rating-desc':
                    filteredProducts.sort((a, b) => (b.rating || 0) - (a.rating || 0));
                    break;
            }
            displayProducts(filteredProducts);
            
            if (currentUser) {
                loadAndUpdateSaveStatus(currentUser, 'products');
            }
        }

        filters.forEach(filter => {
            if (filter) {
                filter.addEventListener('change', applyFiltersAndSort);
            }
        });
    }

    function setupQuickView(products) {
         if(productGrid) {
            productGrid.addEventListener('click', (event) => {
                if (event.target.closest('.save-btn')) {
                    return;
                }
                
                const button = event.target.closest('.quick-view-btn');
                if (button) {
                    const productLink = button.dataset.productLink;
                    const product = products.find(p => p.link === productLink);
                    if (product) {
                        const priceDisplay = product.price.toLowerCase().includes('free') || product.price.includes('£') ? product.price : `£${product.price}`;
                        const modalHTML = `
                            <div class="quick-view-modal">
                                <img src="${product.image}" alt="${product.name}" class="quick-view-image">
                                <div class="quick-view-info">
                                    <h3 class="quick-view-title">${product.name}</h3>
                                    <p class="quick-view-brand">${product.brand}</p>
                                    <p class="quick-view-price">${priceDisplay}</p>
                                    <a href="${product.link}" target="_blank" rel="noopener sponsored" class="quick-view-button">View Product</a>
                                    <p class="quick-view-description">${product.description}</p>
                                </div>
                            </div>
                        `;
                        openModal(product.name, modalHTML);
                    }
                }
            });
         }
    }

    function displayRecipes(recipes, containerId) {
        const grid = document.getElementById(containerId);
        if (!grid) return;
        grid.innerHTML = ''; 

        if (recipes.length === 0) {
            grid.innerHTML = '<p class="empty-message">You haven\'t saved any recipes yet. Click the save icon on a recipe to add it here!</p>';
            return;
        }

        recipes.forEach(recipe => {
            const card = document.createElement('article');
            card.className = 'recipe-card';
            const safeId = btoa(recipe.href); 
            const imagePath = recipe.image.startsWith('../') ? recipe.image.substring(3) : recipe.image;

            card.innerHTML = `
                <a href="${recipe.href}">
                    <button class="save-btn" title="Save recipe" 
                            data-item-id="${safeId}" 
                            data-item-type="recipes"
                            data-name="${recipe.name}"
                            data-description="${recipe.description}"
                            data-image="${recipe.image}" 
                            data-href="${recipe.href}">
                        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
                    </button>
                    <img src="${imagePath}" alt="${recipe.name}">
                    <div class="card-content">
                        <h3>${recipe.name}</h3>
                        <p>${recipe.description}</p>
                    </div>
                </a>
            `;
            grid.appendChild(card);
        });
    }

    async function loadSavedRecipes(user) {
        const container = document.getElementById('saved-recipes-grid');
        if (!user || !container || typeof db === 'undefined') return;

        try {
            const snapshot = await db.collection('user_saves').doc(user.uid).collection('recipes').get();
            if (snapshot.empty) {
                console.log('No saved recipes found.');
                container.innerHTML = '<p class="empty-message">You haven\'t saved any recipes yet. Click the save icon on a recipe to add it here!</p>';
                return;
            }
            const savedRecipes = [];
            snapshot.forEach(doc => {
                savedRecipes.push(doc.data());
            });
            displayRecipes(savedRecipes, 'saved-recipes-grid');
            
            const allButtons = container.querySelectorAll(`.save-btn[data-item-type="recipes"]`);
            allButtons.forEach(button => {
                toggleSaveState(button, true);
            });
        } catch (error) {
            console.error("Error loading saved recipes: ", error);
            container.innerHTML = '<p class="empty-message">Error loading saved recipes. Please try again later.</p>';
        }
    }

    async function loadSavedProducts(user) {
        const container = document.getElementById('saved-products-grid');
        if (!user || !container || typeof db === 'undefined') return;

        try {
            const snapshot = await db.collection('user_saves').doc(user.uid).collection('products').get();
            if (snapshot.empty) {
                console.log('No saved products found.');
                container.innerHTML = '<p class="empty-message">You haven\'t saved any products yet. Click the save icon on a product to add it here!</p>';
                return;
            }
            const savedProducts = [];
            snapshot.forEach(doc => {
                savedProducts.push(doc.data());
            });
            displayProducts(savedProducts, 'saved-products-grid');
            
            const allButtons = container.querySelectorAll(`.save-btn[data-item-type="products"]`);
            allButtons.forEach(button => {
                toggleSaveState(button, true);
            });
        } catch (error) {
            console.error("Error loading saved products: ", error);
            container.innerHTML = '<p class="empty-message">Error loading saved products. Please try again later.</p>';
        }
    }
    
    function displaySavedJobs(jobs, containerId) {
        const grid = document.getElementById(containerId);
        if (!grid) return;
        grid.innerHTML = ''; 

        if (jobs.length === 0) {
            grid.innerHTML = '<p class="empty-message">You haven\'t saved any jobs yet. Click the save icon on a job to add it here!</p>';
            return;
        }

        jobs.forEach(job => {
            const card = document.createElement('div');
            card.className = 'job-card'; 
            const safeId = btoa(job.id); 

            card.innerHTML = `
                <button class="save-btn" title="Unsave job" 
                        data-item-id="${safeId}" 
                        data-item-type="jobs"
                        data-name="${job.name}"
                        data-company="${job.company}" 
                        data-location="${job.location}">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
                </button>
                <a href="jobs.html?job=${job.id}" class="job-dashboard-link" style="text-decoration: none;">
                    <h4>${job.name}</h4>
                    <p>${job.company} | ${job.location}</p>
                </a>
            `;
            grid.appendChild(card);
        });
    }

    async function loadSavedJobs(user) {
        const container = document.getElementById('saved-jobs-grid');
        if (!user || !container || typeof db === 'undefined') return;

        try {
            const snapshot = await db.collection('user_saves').doc(user.uid).collection('jobs').get();
            if (snapshot.empty) {
                console.log('No saved jobs found.');
                container.innerHTML = '<p class="empty-message">You haven\'t saved any jobs yet. Click the save icon on a job to add it here!</p>';
                return;
            }
            const savedJobs = [];
            snapshot.forEach(doc => {
                savedJobs.push(doc.data());
            });
            
            displaySavedJobs(savedJobs, 'saved-jobs-grid');
            
            const allButtons = container.querySelectorAll(`.save-btn[data-item-type="jobs"]`);
            allButtons.forEach(button => {
                toggleSaveState(button, true);
            });
        } catch (error) {
            console.error("Error loading saved jobs: ", error);
            container.innerHTML = '<p class="empty-message">Error loading saved jobs. Please try again later.</p>';
        }
    }


    // ===================================
    // === 4. PAGE-SPECIFIC INITIALIZERS ===
    // (Code that runs on specific pages)
    // ===================================

    // --- Mobile Menu Toggle ---
    const menuToggle = document.querySelector('.mobile-menu-toggle');
    const secondaryNav = document.querySelector('.secondary-nav');
    if (menuToggle && secondaryNav) {
        menuToggle.addEventListener('click', () => {
            secondaryNav.classList.toggle('active');
            const isExpanded = secondaryNav.classList.contains('active');
            menuToggle.setAttribute('aria-expanded', isExpanded);
        });
    }

    // --- Footer links ---
    const aboutLink = document.getElementById('about-link');
    const contactLink = document.getElementById('contact-link');
    if (aboutLink) {
        aboutLink.addEventListener('click', (e) => {
            e.preventDefault();
            openModal('About Us', '<p>TeaTrade is your central hub for the latest news, job opportunities, and products in the tea industry. We connect professionals and enthusiasts alike.</p>');
        });
    }
    if (contactLink) {
        contactLink.addEventListener('click', (e) => {
            e.preventDefault();
            openModal('Contact Us', '<p>For inquiries, please reach out via email: <a href="mailto:info@teatrade.co.uk">info@teatrade.co.uk</a>.</p>');
        });
    }

    // --- Products Page Logic ---
    if (productGrid) { 
        fetch('products.json')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(products => {
                allProducts = products; // Store products globally
                populateFilters(products);
                displayProducts(products); 
                setupFiltering(products);
                setupQuickView(products);
            })
            .catch(error => {
                console.error('Error fetching products:', error);
                productGrid.innerHTML = '<p style="padding: 24px; color: red;">Error loading products. Please try again later.</p>';
            });
    } // End of if(productGrid) check


    // --- Recipe Page Filter & Search ---
    const recipeFilterBar = document.querySelector('.recipes-page .filter-bar');
    if (recipeFilterBar) {
        const recipeSearchInput = document.querySelector('.header-search-bar'); 
        const teaFilter = document.querySelector('#filter-tea-type');
        const courseFilter = document.querySelector('#filter-course');
        const allCards = document.querySelectorAll('.recipe-card');

        function filterRecipes() {
            const searchTerm = recipeSearchInput ? recipeSearchInput.value.toLowerCase() : '';
            const selectedTea = teaFilter ? teaFilter.value : 'all';
            const selectedCourse = courseFilter ? courseFilter.value : 'all';

            allCards.forEach(card => {
                const cardTitle = card.querySelector('h3').textContent.toLowerCase();
                const cardTeaType = card.dataset.tea;
                const cardCourseType = card.dataset.course;

                const teaMatch = (selectedTea === 'all') || (cardTeaType === selectedTea);
                const courseMatch = (selectedCourse === 'all') || (cardCourseType === selectedCourse);
                const searchMatch = cardTitle.includes(searchTerm);

                if (teaMatch && courseMatch && searchMatch) {
                    card.style.display = 'block';
                } else {
                    card.style.display = 'none';
                }
            });
        }

        if(recipeSearchInput) recipeSearchInput.addEventListener('input', filterRecipes);
        if(teaFilter) teaFilter.addEventListener('change', filterRecipes);
        if(courseFilter) courseFilter.addEventListener('change', filterRecipes);
    } 

    // --- Jobs Page Logic ---
    if (document.body.classList.contains('jobs-page')) {
        // All logic is now in the inline script in jobs.html
        // We just need to check for the URL parameter
        
        const urlParams = new URLSearchParams(window.location.search);
        const jobToOpen = urlParams.get('job');
        if (jobToOpen) {
            // We need to wait for the jobs to be rendered.
            // Use MutationObserver to wait for the job list
            const jobListPane = document.getElementById('job-list-container');
            if (jobListPane) {
                const observer = new MutationObserver((mutationsList, obs) => {
                    const cardToActivate = document.querySelector(`.job-card[data-job="${jobToOpen}"]`);
                    if (cardToActivate) {
                        cardToActivate.click();
                        obs.disconnect(); // We found it, stop observing
                    }
                });
                // Observe changes to the children of the job list pane
                observer.observe(jobListPane, { childList: true });
            }
        }
    }


    // --- Corporate Page "Coming Soon" Modal Links ---
    const comingSoonLinks = document.querySelectorAll('.modal-link-soon');
    comingSoonLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault(); 
            const pageTitle = link.textContent;
            openModal(pageTitle + ' - Restricted Access', '<p>This feature is available to professionals of the tea trade and only permitted through application. Please contact <a href="mailto:info@teatrade.co.uk">info@teatrade.co.uk</a> for more information.</p>');
        });
    });

    // --- Auth Forms (Login & Signup) ---
    if (typeof auth !== 'undefined') {
        const loginForm = document.getElementById('login-form');
        if (loginForm) {
            const loginEmailInput = document.getElementById('login-email');
            const loginPasswordInput = document.getElementById('login-password');
            const loginErrorElement = document.getElementById('login-error');
            const loginButton = loginForm.querySelector('.auth-button');

            const urlParams = new URLSearchParams(window.location.search);
            if (urlParams.get('status') === 'signup-success') {
                const successMessage = document.createElement('p');
                successMessage.className = 'form-success-message';
                successMessage.textContent = 'Account created successfully! Please sign in.';
                loginForm.prepend(successMessage);
            }

            loginForm.addEventListener('submit', (e) => {
                e.preventDefault();
                const email = loginEmailInput.value;
                const password = loginPasswordInput.value;
                loginButton.disabled = true;
                loginButton.textContent = "Signing in...";
                loginErrorElement.style.display = 'none';

                auth.signInWithEmailAndPassword(email, password)
                    .then((userCredential) => {
                        console.log('User signed in:', userCredential.user);
                        window.location.href = 'index.html'; 
                    })
                    .catch((error) => {
                        console.error('Login Error:', error.message);
                        loginErrorElement.textContent = "Invalid email or password. Please try again.";
                        loginErrorElement.style.display = 'block';
                        loginButton.disabled = false;
                        loginButton.textContent = "Sign In";
                    });
            });
        }

        const signupForm = document.getElementById('signup-form');
        if (signupForm) {
            const signupEmailInput = document.getElementById('signup-email');
            const signupPasswordInput = document.getElementById('signup-password');
            const signupErrorElement = document.getElementById('signup-error');
            const signupButton = signupForm.querySelector('.auth-button');

            signupForm.addEventListener('submit', (e) => {
                e.preventDefault();
                const email = signupEmailInput.value;
                const password = signupPasswordInput.value;
                signupButton.disabled = true;
                signupButton.textContent = "Creating account...";
                signupErrorElement.style.display = 'none';

                auth.createUserWithEmailAndPassword(email, password)
                    .then((userCredential) => {
                        console.log('User created:', userCredential.user);
                        window.location.href = 'login.html?status=signup-success';
                    })
                    .catch((error) => {
                        console.error('Signup Error:', error.message);
                        signupErrorElement.textContent = error.message;
                        signupErrorElement.style.display = 'block';
                        signupButton.disabled = false;
                        signupButton.textContent = "Create Account";
                    });
            });
        }
    } else {
        console.error("Firebase 'auth' is not defined. Login/Signup forms will not work.");
    }
    
    // --- Dashboard Page Logic ---
    if (document.body.classList.contains('dashboard-page')) {
        const tabs = document.querySelectorAll('.dashboard-tab-button');
        const panels = document.querySelectorAll('.dashboard-panel');

        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const target = tab.dataset.tab;
                tabs.forEach(t => t.classList.remove('active'));
                panels.forEach(p => p.classList.remove('active'));
                tab.classList.add('active');
                document.getElementById('tab-' + target).classList.add('active');
            });
        });
    }

    // --- *** NEW: Article Page Share Button *** ---
    const copyLinkBtn = document.querySelector('.copy-link-btn');
    if (copyLinkBtn) {
        copyLinkBtn.addEventListener('click', (e) => {
            e.preventDefault();
            const url = window.location.href;
            navigator.clipboard.writeText(url).then(() => {
                // Use the existing modal function to show success
                openModal('Link Copied!', '<p>The article link has been copied to your clipboard.</p>');
            }).catch(err => {
                console.error('Failed to copy link: ', err);
            });
        });
    }
    
    // ===================================
    // === 5. GLOBAL AUTH LISTENER (RUNS LAST) ===
    // This is the *last* thing to run. It ensures all other
    // page logic has been set up first.
    // ===================================
    
    if (typeof auth !== 'undefined') {
        auth.onAuthStateChanged(user => {
            const userActionsDesktop = document.querySelector('.site-header .user-actions, .home-header-wrapper .user-actions, .corporate-header-wrapper .user-actions');
            const secondaryNavUl = document.querySelector('.secondary-nav ul');
            const prefix = document.body.classList.contains('recipe-detail-page') ? '../' : './';

            if (user) {
                // --- USER IS LOGGED IN ---
                if (userActionsDesktop) {
                    userActionsDesktop.innerHTML = `
                        <a href="${prefix}dashboard.html" title="My Library">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
                        </a>
                        <button id="sign-out-btn" title="Sign Out">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg>
                        </button>
                    `;
                    const signOutBtn = document.getElementById('sign-out-btn');
                    if (signOutBtn) {
                        signOutBtn.addEventListener('click', () => {
                            auth.signOut().then(() => {
                                window.location.href = 'index.html';
                            }).catch((error) => {
                                console.error('Sign out error', error);
                            });
                        });
                    }
                }
                
                if (secondaryNavUl && !secondaryNavUl.querySelector('.my-library-link')) {
                    const libraryLink = document.createElement('li');
                    libraryLink.className = 'my-library-link';
                    if (document.body.classList.contains('dashboard-page')) {
                        libraryLink.innerHTML = `<a href="dashboard.html" class="active">My Library</a>`;
                    } else {
                        libraryLink.innerHTML = `<a href="${prefix}dashboard.html">My Library</a>`;
                    }
                    secondaryNavUl.appendChild(libraryLink);
                }
                
                // Trigger content loading for logged-in user
                initializeUserContent(user);

            } else {
                // --- USER IS LOGGED OUT ---
                if (userActionsDesktop) {
                    userActionsDesktop.innerHTML = `
                        <a href="${prefix}signup.html" title="Create Account"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="8.5" cy="7" r="4"></circle><line x1="20" y1="8" x2="20" y2="14"></line><line x1="17" y1="11" x2="23" y2="11"></line></svg></a>
                        <a href="${prefix}login.html" title="Sign In"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"></path><polyline points="10 17 15 12 10 7"></polyline><line x1="15" y1="12" x2="3" y2="12"></line></svg></a>
                    `;
                }

                const libraryLink = secondaryNavUl ? secondaryNavUl.querySelector('.my-library-link') : null;
                if (libraryLink) {
                    libraryLink.remove();
                }

                if (document.body.classList.contains('dashboard-page')) {
                    window.location.href = 'login.html';
                }
                
                // Initialize for logged-out user (so save buttons redirect to login)
                initializeUserContent(null);
            }
        });
    } else {
        console.error("Firebase 'auth' object is not defined. Scripts might be in the wrong order.");
        // If auth fails, still try to load products for logged-out users
        // This is the fallback that fixes the "Loading products..." bug
        if (productGrid) {
            fetch('products.json')
            .then(response => {
                if(!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(products => {
                allProducts = products;
                populateFilters(products);
                displayProducts(products);
                setupFiltering(products);
                setupQuickView(products);
                initializeUserContent(null); // Init save buttons for logged-out state
            }).catch(e => {
                console.error("Fetch failed", e);
                productGrid.innerHTML = '<p style="padding: 24px; color: red;">Error loading products. Please try again later.</p>';
            });
        }
    }

}); // End DOMContentLoaded