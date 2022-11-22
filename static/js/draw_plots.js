import { openFile, draw } from 'https://root.cern/js/latest/modules/main.mjs';
import { gStyle } from 'https://root.cern/js/latest/modules/main.mjs';



var run_nums = document.querySelector('#run_nums').value
console.log(run_nums)

let included_runs = document.createElement('div');
included_runs.textContent = 'Included runs: ' + run_nums;
included_runs['style'] = 'justify-content: left;'
let larger_container = document.getElementById("main_container");

run_nums = run_nums.split(',').map(element => element.trim());
var plot_types = document.querySelector('#plot_types').value;
plot_types = plot_types.split(',').map(element => element.trim());
console.log(plot_types)
console.log(run_nums)
plot_types.forEach(draw_plot_type);
larger_container.prepend(included_runs);


async function draw_plot_type(plot_type) {
    let container = document.createElement('div');
    container.id = plot_type + "_container";
    container.classList.add("container");
    let draw_container = false;
    run_nums.forEach(draw_run_num);
    
    if (draw_container) larger_container.prepend(container);

    async function draw_run_num(run_num) {
        if (run_num === '') return;
        draw_container = true;
        let objName = "drawing" + plot_type.split("/").slice(2).join("_") + "_" + run_num
        let drawing = document.createElement('div');
        drawing.id = objName
        drawing.classList.add("drawing")
        container.appendChild(drawing);

        gStyle.fStatFormat = "7.5g";
        let filename = './data.root';
        let file = await openFile(filename);
        console.log(plot_type + "/" + run_num + ';1')
        let obj = await file.readObject( plot_type + "/" + run_num + ';1');

        if (obj._typename.includes('TH2')) await draw(objName, obj, "colz");
        else await draw(objName, obj, "hist");
        

    }
}