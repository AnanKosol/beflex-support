(function initDarkModeToggle() {
  const storageKey = 'beflexSupportTheme';

  function getInitialTheme() {
    const savedTheme = localStorage.getItem(storageKey);
    if (savedTheme === 'dark' || savedTheme === 'light') {
      return savedTheme;
    }
    // ใช้ค่าเริ่มต้น light theme
    return 'light';
  }

  function applyTheme(theme, button) {
    const nextTheme = theme === 'dark' ? 'dark' : 'light';
    document.body.classList.toggle('theme-dark', nextTheme === 'dark');
    document.body.dataset.theme = nextTheme;
    localStorage.setItem(storageKey, nextTheme);

    if (button) {
      button.textContent = nextTheme === 'dark' ? '☀️ Light' : '🌙 Dark';
      button.setAttribute('aria-label', nextTheme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode');
      button.title = nextTheme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
    }
  }

  function mountToggle() {
    if (!document.body || document.querySelector('.theme-toggle')) {
      return;
    }

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'theme-toggle';
    button.addEventListener('click', () => {
      const nextTheme = document.body.classList.contains('theme-dark') ? 'light' : 'dark';
      applyTheme(nextTheme, button);
    });

    document.body.appendChild(button);
    applyTheme(getInitialTheme(), button);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mountToggle, { once: true });
    return;
  }

  mountToggle();
})();