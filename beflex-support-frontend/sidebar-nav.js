(function renderSidebarMenu() {
  const navContainer = document.querySelector('.sidebar .nav-links');
  if (!navContainer) {
    return;
  }

  const currentPage = document.body.dataset.navPage || '';
  const menuItems = [
    { id: 'service', href: 'service.html', label: 'Import Permission' },
    { id: 'group-service', href: 'group-service.html', label: 'Import User to Group' },
    { id: 'user-csv-service', href: 'user-csv-service.html', label: 'Create user csv' },
    { id: 'pm-service', href: 'pm.html', label: 'PM' },
    { id: 'support-other', href: 'support-other.html', label: 'Support Other', support: true }
  ];

  navContainer.innerHTML = menuItems
    .map((item) => {
      const classes = ['nav-item'];
      if (item.support) {
        classes.push('nav-item-support-page');
      }
      if (item.id === currentPage) {
        classes.push('active');
      }

      return `<a class="${classes.join(' ')}" href="${item.href}">${item.label}</a>`;
    })
    .join('');
})();
