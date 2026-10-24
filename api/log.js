// Visitor log relay. The page posts each event here instead of straight to
// the Google sheet. Vercel hands this function a coarse location for the
// visitor (city, region, country). The sheet writes the "ua" field to its
// column E, so the location goes there; the browser string and device token
// already travel in "ref" (column F). The parts are also sent as their own
// fields. No address is read or forwarded. The event then goes on to the
// sheet unchanged otherwise. Anything that is not JSON is forwarded as is.
const SHEET_ENDPOINT = 'https://script.google.com/macros/s/AKfycbxLCj-MK2jWuzQw2H5NDZacRI1_ZV_Fjv-mUVWQ8-O4pHg2r3JfXaSBWBGqrb-YryDyOA/exec';
const MAX_BODY = 8192;

function readBody(req) {
    if (typeof req.body === 'string') return Promise.resolve(req.body);
    if (req.body && typeof req.body === 'object') return Promise.resolve(JSON.stringify(req.body));
    return new Promise((resolve, reject) => {
        let data = '';
        req.on('data', chunk => { data += chunk; if (data.length > MAX_BODY) { reject(new Error('too large')); req.destroy(); } });
        req.on('end', () => resolve(data));
        req.on('error', reject);
    });
}

function header(req, name) {
    const v = req.headers[name];
    return Array.isArray(v) ? v[0] : (v || '');
}

function decode(v) {
    try { return decodeURIComponent(v); } catch (e) { return v; }
}

function clientPlace(req) {
    const city = decode(header(req, 'x-vercel-ip-city'));
    const region = decode(header(req, 'x-vercel-ip-country-region'));
    const country = decode(header(req, 'x-vercel-ip-country'));
    return { city, region, country, label: [city, region, country].filter(Boolean).join(', ') };
}

module.exports = async (req, res) => {
    if (req.method !== 'POST') { res.statusCode = 405; res.setHeader('Allow', 'POST'); res.end(); return; }
    let body;
    try { body = await readBody(req); } catch (e) { res.statusCode = 413; res.end(); return; }
    if (!body || body.length > MAX_BODY) { res.statusCode = body ? 413 : 400; res.end(); return; }

    const place = clientPlace(req);
    let out = body;
    try {
        const d = JSON.parse(body);
        if (d && typeof d === 'object') {
            d.ua = place.label;   // sheet column E
            d.city = place.city;
            d.region = place.region;
            d.country = place.country;
            out = JSON.stringify(d);
        }
    } catch (e) { /* not JSON: forward untouched */ }

    try {
        const r = await fetch(SHEET_ENDPOINT, {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain;charset=utf-8' },
            body: out,
            redirect: 'follow'
        });
        // Apps Script answers 200 (or a 302 to a 200) once the row is written
        res.statusCode = r.ok ? 204 : 502;
        res.end();
    } catch (e) {
        res.statusCode = 502;
        res.end();
    }
};
