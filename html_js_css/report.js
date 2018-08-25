let curr_hash = "";

function clicked(el_id) {
    if (el_id === "")
        el_id = "cluster_summary";

    const el = document.getElementById(el_id);
    if (el === null) {
        alert("Element '" + el_id + "' is not defined")
    } else {
        for(const old of document.getElementsByClassName("main-ceph")) {
            old.classList.remove('main-ceph');
            old.classList.add('data-ceph');
        }

        el.classList.remove('data-ceph');
        el.classList.add('main-ceph');
        curr_hash = el_id;
        window.location.hash = el_id;
        window.scrollTo(0, 0);
    }

    const menu_el = document.getElementById("ptr_" + el_id);
    if (menu_el !== null) {
        for(const el of document.getElementsByClassName("selected")) {
            if (el.classList.contains("menulink")) {
                el.classList.remove('selected');
            }
        }
        menu_el.classList.add('selected');
    }
}

function onHashChanged() {
    if ("#" + curr_hash !== window.location.hash)
        clicked(window.location.hash.substr(1));
}


function setColors(cmap, divid, linkid) {
    for (const [itemId, color] of Object.entries(cmap)) {
        document.querySelector("#" + itemId + " ellipse").style.fill = color;
    }

    for(const el of document.querySelectorAll("div#" + divid + " span.selected"))
        el.classList.remove('selected');

    document.getElementById(linkid).classList.add('selected');
}

window.onhashchange = onHashChanged;
