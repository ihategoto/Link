const selected = document.querySelector(".selected");
const optionsContainer = document.querySelector(".options-container");
const searchBox = document.querySelector(".search-input");

const optionsList = document.querySelectorAll(".option");


//console.log("Working main");

selected.addEventListener("click", () => {
  optionsContainer.classList.toggle("active");
});

//console.log(optionsList);
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
