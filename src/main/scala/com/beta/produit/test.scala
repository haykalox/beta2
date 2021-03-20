package com.beta.produit

import com.typesafe.config._

object test {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load("application.conf").getConfig("tables")
    val commande = config.getConfig("commande")
    val testG = commande.getString("Commentaire")
    val categories = config.getConfig("categories")
    val produits = config.getConfig("produits")
    val clients = config.getConfig("clients")

    println(commande)
    println("***************************************")
    println(produits)
    println("***************************************")
    println(categories)
    println("***************************************")
    println(clients)
    println("-----------------------------------------")
println(testG)
  }
}
