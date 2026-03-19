(function renderSidebarMenu() {
  // Toggle this flag to show/hide PM menu without touching other files.
  const SHOW_PM = false;

  const navContainer = document.querySelector('.sidebar .nav-links');
  if (!navContainer) {
    return;
  }

  const currentPage = document.body.dataset.navPage || '';
  const menuItems = [
    { id: 'service', href: '/support/service.html', label: 'Import Permission' },
    { id: 'group-service', href: '/support/group-service.html', label: 'Import User to Group' },
    { id: 'query-add-permission-service', href: '/support/query-add-permission.html', label: 'Query & add permission' },
    { id: 'user-csv-service', href: '/support/user-csv-service.html', label: 'Create user csv' },
    { id: 'query-sizing-service', href: '/support/query-sizing.html', label: 'Query Sizing file' },
    { id: 'audit-service', href: '/support/audit.html', label: 'Audit' },
    { id: 'support-other', href: '/support/support-other.html', label: 'Support Other', support: true }
  ];

  if (SHOW_PM) {
    menuItems.splice(3, 0, { id: 'pm-service', href: '/support/pm.html', label: 'PM' });
  }

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
