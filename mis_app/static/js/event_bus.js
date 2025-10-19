// Global event bus for lightweight pub/sub across widgets
(function (w) {
	const evt = {};
	w.eventBus = {
		on(name, cb) {
			(evt[name] = evt[name] || []).push(cb);
		},
		emit(name, payload) {
			(evt[name] || []).forEach((cb) => cb(payload));
		},
	};
})(window);
