/*
 * TeaTrade CORPORATE Script
 * A lighter version of the main script, focused on jobs, auth, and modals.
 */

// ===================================
// === GLOBAL HELPER FUNCTIONS ===
// ===================================
function toggleSaveState(button, isSaved) {
    if (isSaved) {
        button.classList.add('saved');
        button.title = "Unsave item";
    } else {
        button.classList.remove('saved');
        button.title = "Save item";
    }
}

async function loadAndUpdateSaveStatus(user, itemType) {
    if (!user || typeof db === 'undefined') { 
        console.log("User or db not ready for loadAndUpdateSaveStatus");
        return; 
    }
    const savedItemIds = new Set();
    try {
        const snapshot = await db.collection('user_saves').doc(user.uid).collection(itemType).get();
        snapshot.forEach(doc => { savedItemIds.add(doc.id); });
        if (savedItemIds.size > 0) {
            const allButtons = document.querySelectorAll(`.save-btn[data-item-type="${itemType}"]`);
            allButtons.forEach(button => {
                if (savedItemIds.has(button.dataset.itemId)) {
                    toggleSaveState(button, true);
                }
            });
        }
    } catch (error) { console.error("Error loading saved statuses: ", error); }
}


document.addEventListener('DOMContentLoaded', () => {

    // ===================================
    // === 1. GLOBAL VARIABLES & HELPERS ===
    // ===================================
    const modalOverlay = document.getElementById('info-modal');
    const modalContent = document.getElementById('modal-body-content');
    const closeModalBtn = document.getElementById('modal-close-btn');

    function openModal(title, content) {
        if (!modalOverlay || !modalContent) return;
        modalContent.innerHTML = `<h2>${title}</h2>${content}`;
        modalOverlay.classList.add('active');
    }

    function closeModal() {
        if (!modalOverlay) return;
        modalOverlay.classList.remove('active');
        modalContent.innerHTML = ''; 
    }

    if (closeModalBtn) { closeModalBtn.addEventListener('click', closeModal); }
    if (modalOverlay) {
        modalOverlay.addEventListener('click', (event) => {
            if (event.target === modalOverlay) { closeModal(); }
        });
    }

    // ===================================
    // === 2. SAVE & DASHBOARD FUNCTIONS ===
    // ===================================

    async function handleSaveClick(e, button, user) {
        e.preventDefault(); 
        e.stopPropagation(); 
        
        if (!user) {
            window.location.href = './login.html'; 
            return;
        }
        if (typeof db === 'undefined') { console.error("Database (db) is not defined."); return; }

        const itemId = button.dataset.itemId; 
        const itemType = button.dataset.itemType; 
        const itemRef = db.collection('user_saves').doc(user.uid).collection(itemType).doc(itemId);

        try {
            const doc = await itemRef.get();
            if (doc.exists) {
                await itemRef.delete();
                toggleSaveState(button, false);
                console.log('Item unsaved!');
                if (document.body.classList.contains('dashboard-page')) {
                    const card = button.closest('.job-card');
                    if (card) { card.remove(); }
                }
            } else {
                let itemData;
                if (itemType === 'jobs') {
                    itemData = {
                        id: atob(itemId), 
                        name: button.dataset.name,
                        company: button.dataset.company,
                        location: button.dataset.location
                    };
                }
                if (itemData) {
                    await itemRef.set(itemData); 
                    toggleSaveState(button, true);
                    console.log('Item saved!');
                } else {
                    console.warn('Could not find item data to save for ID:', itemId);
                }
            }
        } catch (error) { console.error("Error saving item: ", error); }
    }

    function initializeUserContent(user) {
        const saveButtonContainers = document.querySelectorAll('.job-list-pane, #saved-jobs-grid');

        if (saveButtonContainers.length > 0) {
            saveButtonContainers.forEach(container => {
                container.addEventListener('click', (e) => {
                    const saveBtn = e.target.closest('.save-btn');
                    if (saveBtn) {
                        handleSaveClick(e, saveBtn, user);
                    }
                });
            });
        }
        
        if (user) {
            if (document.body.classList.contains('jobs-page')) {
                loadAndUpdateSaveStatus(user, 'jobs');
            }
            if (document.body.classList.contains('dashboard-page')) {
                loadSavedJobs(user); 
            }
        }
    }
    
    // ===================================
    // === 3. PAGE-SPECIFIC FUNCTIONS ===
    // ===================================
    
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

    // --- Jobs Page Logic ---
    if (document.body.classList.contains('jobs-page')) {
        const urlParams = new URLSearchParams(window.location.search);
        const jobToOpen = urlParams.get('job');
        if (jobToOpen) {
            const jobListPane = document.getElementById('job-list-container');
            if (jobListPane) {
                const observer = new MutationObserver((mutationsList, obs) => {
                    const cardToActivate = document.querySelector(`.job-card[data-job="${jobToOpen}"]`);
                    if (cardToActivate) {
                        cardToActivate.click();
                        obs.disconnect(); 
                    }
                });
                observer.observe(jobListPane, { childList: true });
            }
        }
    }

    // --- Corporate Page "Coming Soon" Modal Links ---
    // *** THIS IS THE FIX FOR YOUR MODALS ***
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
    
    // ===================================
    // === 5. GLOBAL AUTH LISTENER (RUNS LAST) ===
    // ===================================
    
    if (typeof auth !== 'undefined') {
        auth.onAuthStateChanged(user => {
            const userActionsDesktop = document.querySelector('.site-header .user-actions, .home-header-wrapper .user-actions, .corporate-header-wrapper .user-actions');
            const secondaryNavUl = document.querySelector('.secondary-nav ul');
            const prefix = './'; // Simpler pathing on this site

            if (user) {
                // --- USER IS LOGGED IN ---
                if (userActionsDesktop) {
                    userActionsDesktop.innerHTML = `
                        <a href="${prefix}dashboard.html" title="My Library">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24" 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
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
                
                initializeUserContent(null);
            }
        });
    } else {
        console.error("Firebase 'auth' object is not defined. Scripts might be in the wrong order.");
    }

}); // End DOMContentLoaded