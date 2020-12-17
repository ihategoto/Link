//barra dei nodi
const selected = document.querySelector(".selected");
const optionsContainer = document.querySelector(".options-container");
const searchBox = document.querySelector(".search-input");
const optionsList = document.querySelectorAll(".option");

//bottoni vent on/off
const vent_on = document.querySelector(".vent_on");
const vent_off = document.querySelector(".vent_off");

//console.log("Working main");

//Attivo e disattivo i rispettivi pulsanti
vent_on.addEventListener("click", () => {
  vent_on.disabled = true;
  vent_on.style.background = "#414b57";
  vent_off.disabled = false;
  vent_off.style.background = "#2f3640";
});

vent_off.addEventListener("click", () => {
  vent_off.disabled = true;
  vent_off.style.background = "#414b57";
  vent_on.disabled = false;
  vent_on.style.background = "#2f3640";
});

/*Controllo se il nodo e' stato appena cliccato o no
  in modo da aggiornare lo stato dei bottoni solo una volta
  per non creare problemi di parallelismo*/
optionsList.forEach(o => {
  o.addEventListener("click", () => {
    optionsContainer.classList.remove("active");
    document.getElementById("clickFlag").value = 0;
  });
});

selected.addEventListener("click", () => {
  optionsContainer.classList.toggle("active");
});

optionsList.forEach(o => {
  o.addEventListener("click", () => {
    //selected.innerHTML = o.querySelector(".label").innerHTML;
    optionsContainer.classList.remove("active");
  });
});

/*** Funzione di ricerca nodo ***/
//const searchBox = document.querySelector(".search-box input");
searchBox.addEventListener("keyup", function(e) {
  filterList(e.key);
  //filterList(e.target.value);
  //console.log(e.target.value);
});

const filterList = searchTerm => {
  searchTerm = searchTerm.toLowerCase();
  optionsList.forEach( option => {
    let label = option.firstElementChild.innerText.toLowerCase();
    if (label.indexOf(searchTerm) != -1) {
      option.style.display = "block";
    } else {
      option.style.display = "none";
    }
  });
};

