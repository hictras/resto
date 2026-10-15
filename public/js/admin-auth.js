// admin-auth.js — shared login gate for all admin pages

const ADMIN_TOKEN_KEY = 'admin_token';

const adminAuth = {
    // Get the current token
    getToken() {
        return localStorage.getItem(ADMIN_TOKEN_KEY);
    },

    // Set / clear
    setToken(t) {
        localStorage.setItem(ADMIN_TOKEN_KEY, t);
    },
    clearToken() {
        localStorage.removeItem(ADMIN_TOKEN_KEY);
    },

    // Verify with the server (returns true / false)
    async verify() {
        const token = this.getToken();
        if (!token) return false;
        try {
            const res = await fetch(`/api/admin-check?token=${encodeURIComponent(token)}`);
            if (!res.ok) return false;
            const data = await res.json();
            return !!data.success;
        } catch (e) {
            return false;
        }
    },

    // Logout on the server and locally, then redirect to login page
    async logout() {
        const token = this.getToken();
        try {
            await fetch('/api/admin-logout', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ token })
            });
        } catch (e) {}
        this.clearToken();
        window.location.replace('/admin-login.html');
    },

    // Gate: call this on every admin page load.
    // If not logged in -> redirect to login. If yes -> call onReady().
    async requireLogin(onReady) {
        const ok = await this.verify();
        if (!ok) {
            this.clearToken();
            window.location.replace('/admin-login.html');
            return;
        }
        if (typeof onReady === 'function') onReady();
    }
};